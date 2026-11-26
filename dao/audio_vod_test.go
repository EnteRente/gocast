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
	Recording, LiveNow                           bool
}

// TEST_MYSQL_DSN also exercises the row locks on MariaDB using uniquely prefixed tables.
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
		t.Cleanup(func() {
			require.NoError(t, db.Migrator().DropTable(&model.AudioRendition{}, table))
		})
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

		previous := job
		job.Generation = "retry"
		require.NoError(t, d.BeginAudioVOD(ctx, job))
		_, err = d.ResolveAudioVOD(ctx, previous)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
		require.NoError(t, d.FinishAudioVOD(ctx, previous, model.AudioReady, "stale audio"))
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

func TestAudioVODIgnoresObsoleteSource(t *testing.T) {
	for name, change := range map[string]func(*gorm.DB) error{
		"playlist URL changed": func(streams *gorm.DB) error {
			return streams.Where("id = ?", 1).Update("playlist_url", "other").Error
		},
		"stream deleted": func(streams *gorm.DB) error {
			return streams.Delete(&audioTestStream{}, 1).Error
		},
	} {
		t.Run(name, func(t *testing.T) {
			d, streams := audioTestDAO(t)
			ctx := t.Context()
			require.NoError(t, streams.Create(&audioTestStream{Model: gorm.Model{ID: 1}, Recording: true, PlaylistUrl: "original"}).Error)
			job := model.AudioJob{StreamID: 1, Version: model.COMB, Generation: "first"}
			require.NoError(t, d.BeginAudioVOD(ctx, job))
			require.NoError(t, change(streams))

			_, err := d.ResolveAudioVOD(ctx, job)
			require.ErrorIs(t, err, gorm.ErrRecordNotFound)
			require.NoError(t, d.FinishAudioVOD(ctx, job, model.AudioReady, "stale audio"))
			var row model.AudioRendition
			require.NoError(t, d.db.First(&row, "stream_id = ? AND version = ?", job.StreamID, job.Version).Error)
			require.Empty(t, row.PlaylistURL)
		})
	}
}
