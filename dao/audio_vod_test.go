package dao

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/TUM-Dev/gocast/model"
)

// SQLite can't migrate the full stream model's MySQL FULLTEXT indexes.
type audioTestStream struct {
	gorm.Model
	CourseID                                     uint
	PlaylistUrl, PlaylistUrlCAM, PlaylistUrlPRES string
	Recording, LiveNow, Private                  bool
}

// TEST_MYSQL_DSN also exercises the row locks on MariaDB. Only the two
// uniquely prefixed test tables are created in that database.
func audioTestDAO(t *testing.T) (streamsDao, *gorm.DB) {
	t.Helper()
	var dialect gorm.Dialector = sqlite.Open(filepath.Join(t.TempDir(), "audio.db"))
	opts := &gorm.Config{}
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn != "" {
		dialect = mysql.Open(dsn)
		opts.NamingStrategy = schema.NamingStrategy{TablePrefix: fmt.Sprintf("audio_%d_", time.Now().UnixNano())}
	}
	db, err := gorm.Open(dialect, opts)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	table := db.NamingStrategy.TableName("Stream")
	if dsn == "" {
		sqlDB.SetMaxOpenConns(1) // SQLite serializes writers.
	} else {
		t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(&model.AudioRendition{}, table)) })
	}
	streams := db.Table(table).Session(&gorm.Session{})
	require.NoError(t, streams.AutoMigrate(&audioTestStream{}))
	require.NoError(t, db.AutoMigrate(&model.AudioRendition{}))
	return streamsDao{db: db}, streams
}

func TestAudioVODResults(t *testing.T) {
	d, streams := audioTestDAO(t)
	ctx := t.Context()
	require.NoError(t, streams.Create(&audioTestStream{Model: gorm.Model{ID: 1}, CourseID: 7, Recording: true,
		PlaylistUrl: "combined", PlaylistUrlCAM: "camera", PlaylistUrlPRES: "presentation"}).Error)
	for v, playlist := range map[model.StreamVersion]string{model.COMB: "combined", model.CAM: "camera", model.PRES: "presentation"} {
		job := model.AudioJob{StreamID: 1, Version: v, Generation: "first"}
		require.NoError(t, d.BeginAudioVOD(ctx, job))
		got, err := d.ResolveAudioVOD(ctx, job)
		require.NoError(t, err)
		require.Equal(t, AudioSource{CourseID: 7, PlaylistURL: playlist}, got)
		require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioReady, "audio/"+playlist))
		// A second runner finishing the same job can't replace the first result.
		require.NoError(t, d.BeginAudioVOD(ctx, job))
		require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioReady, "duplicate"))
		var row model.AudioRendition
		require.NoError(t, d.db.First(&row, "stream_id = ? AND version = ?", 1, v).Error)
		require.Equal(t, "audio/"+playlist, row.PlaylistURL)
		require.Equal(t, model.AudioReady, row.State)

		job.Generation = "retry"
		require.NoError(t, d.BeginAudioVOD(ctx, job))
		require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioFailed, ""))
		require.NoError(t, d.db.First(&row).Error)
		require.Equal(t, "audio/"+playlist, row.PlaylistURL)
		require.Equal(t, model.AudioFailed, row.State)

		job.Generation = "silent"
		require.NoError(t, d.BeginAudioVOD(ctx, job))
		require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioAbsent, ""))
		require.NoError(t, d.db.First(&row).Error)
		require.Empty(t, row.PlaylistURL)
		require.Equal(t, model.AudioAbsent, row.State)
	}
}

func TestAudioVODReplacementAndDeletion(t *testing.T) {
	d, streams := audioTestDAO(t)
	ctx := t.Context()
	require.NoError(t, streams.Create(&audioTestStream{Model: gorm.Model{ID: 1}, PlaylistUrlPRES: "slides"}).Error)
	require.NoError(t, d.SaveVODSource(ctx, 1, model.CAM, "camera", "v1", true))
	require.NoError(t, streams.Create(&audioTestStream{Model: gorm.Model{ID: 2}, PlaylistUrl: "camera", Recording: true}).Error)
	camera := model.AudioJob{StreamID: 1, Version: model.CAM, Generation: "camera-job"}
	slides := model.AudioJob{StreamID: 1, Version: model.PRES, Generation: "slides-job"}
	copyJob := model.AudioJob{StreamID: 2, Version: model.COMB, Generation: "copy-job"}
	for _, job := range []model.AudioJob{camera, slides, copyJob} {
		require.NoError(t, d.BeginAudioVOD(ctx, job))
	}
	require.NoError(t, d.FinishAudioVOD(ctx, camera, model.AudioReady, "old audio"))
	// Redelivery can arrive after an administrator has already generated the audio.
	require.NoError(t, d.SaveVODSource(ctx, 1, model.CAM, "camera", "v1", false))
	var cameraRow model.AudioRendition
	require.NoError(t, d.db.First(&cameraRow, "stream_id = ? AND version = ?", 1, model.CAM).Error)
	require.Equal(t, "old audio", cameraRow.PlaylistURL)

	camera.Generation = "queued-before-replacement"
	require.NoError(t, d.BeginAudioVOD(ctx, camera))
	done := make(chan error, 1)
	go func() { done <- d.FinishAudioVOD(ctx, copyJob, model.AudioReady, "racing audio") }()
	replaced := d.SaveVODSource(ctx, 1, model.CAM, "camera", "v2", false)
	require.NoError(t, <-done)
	require.NoError(t, replaced)
	for _, job := range []model.AudioJob{camera, copyJob} {
		_, err := d.ResolveAudioVOD(ctx, job)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
		require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioReady, "late audio"))
		var row model.AudioRendition
		require.NoError(t, d.db.First(&row, "stream_id = ? AND version = ?", job.StreamID, job.Version).Error)
		require.Empty(t, row.PlaylistURL)
	}
	_, err := d.ResolveAudioVOD(ctx, slides)
	require.NoError(t, err)
	var stream audioTestStream
	require.NoError(t, streams.First(&stream, 1).Error)
	require.True(t, stream.Private)
	require.Equal(t, "slides", stream.PlaylistUrlPRES)

	require.NoError(t, streams.Delete(&stream).Error)
	require.NoError(t, d.FinishAudioVOD(ctx, slides, model.AudioReady, "deleted"))
	var row model.AudioRendition
	require.NoError(t, d.db.First(&row, "stream_id = ? AND version = ?", 1, model.PRES).Error)
	require.Empty(t, row.PlaylistURL)
	_, err = d.ResolveAudioVOD(ctx, slides)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestSaveVODSourceKeepsStreamVisibility(t *testing.T) {
	d, streams := audioTestDAO(t)
	for _, tc := range []struct {
		name            string
		stream          audioTestStream
		courseIsPrivate bool
	}{
		{"already private before the first recording", audioTestStream{Private: true}, false},
		{"already public when another version arrives", audioTestStream{Recording: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, streams.Create(&tc.stream).Error)
			require.NoError(t, d.SaveVODSource(t.Context(), tc.stream.ID, model.COMB, "video", "v1", tc.courseIsPrivate))
			var saved audioTestStream
			require.NoError(t, streams.First(&saved, tc.stream.ID).Error)
			require.Equal(t, tc.stream.Private, saved.Private)
			require.True(t, saved.Recording)
		})
	}
}
