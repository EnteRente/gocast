package dao

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/TUM-Dev/gocast/model"
)

var vodPlaylistColumn = map[model.StreamVersion]string{
	model.COMB: "playlist_url", model.CAM: "playlist_url_cam", model.PRES: "playlist_url_pres",
}

// ErrRetiredVODRevision tells the manager to acknowledge a superseded upload without finalizing it.
var ErrRetiredVODRevision = errors.New("VOD revision has been replaced")

// AudioSource is the stored video that the manager signs for this job.
type AudioSource struct {
	CourseID    uint
	PlaylistURL string
}

func audioSourceKey(playlist string) []byte {
	sum := sha256.Sum256([]byte(playlist))
	return sum[:]
}

// Lock the parent stream first, as SaveVODSource does. A replacement or a
// deletion cannot slip between reading the video's URL and updating its audio.
func (d streamsDao) withAudioSource(ctx context.Context, job model.AudioJob, work func(*gorm.DB, AudioSource) error) error {
	col, ok := vodPlaylistColumn[job.Version]
	if !ok || job.StreamID == 0 || job.Generation == "" {
		return errors.New("invalid VOD audio job")
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var source AudioSource
		err := tx.Model(&model.Stream{}).Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("course_id, "+col+" AS playlist_url").
			Where("id = ? AND recording = ? AND live_now = ? AND "+col+" <> ''", job.StreamID, true, false).
			Take(&source).Error
		if err != nil {
			return err
		}
		return work(tx, source)
	})
}

func renditionFor(tx *gorm.DB, id uint, version model.StreamVersion) *gorm.DB {
	return tx.Model(&model.AudioRendition{}).Where("stream_id = ? AND version = ?", id, version)
}

// Insert before locking: on InnoDB, SELECT FOR UPDATE on a missing row can
// lock the gap where a concurrent job for a different stream needs to insert.
func lockRendition(tx *gorm.DB, id uint, version model.StreamVersion, key []byte) (model.AudioRendition, error) {
	row := model.AudioRendition{AudioJob: model.AudioJob{StreamID: id, Version: version}, SourceKey: key}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return row, err
	}
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&row).Error
	return row, err
}

func pendingAudio(tx *gorm.DB, job model.AudioJob, source AudioSource) *gorm.DB {
	return renditionFor(tx, job.StreamID, job.Version).
		Where("source_key = ? AND generation = ? AND state = ?", audioSourceKey(source.PlaylistURL), job.Generation, model.AudioPending)
}

func (d streamsDao) BeginAudioVOD(ctx context.Context, job model.AudioJob) error {
	return d.withAudioSource(ctx, job, func(tx *gorm.DB, source AudioSource) error {
		key := audioSourceKey(source.PlaylistURL)
		row, err := lockRendition(tx, job.StreamID, job.Version, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(row.SourceKey, key) {
			row = model.AudioRendition{SourceKey: key}
		} else if row.Generation == job.Generation {
			return nil // redelivering the request must not reopen a finished job
		}
		row.AudioJob, row.State = job, model.AudioPending
		return tx.Save(&row).Error
	})
}

// ResolveAudioVOD returns ErrRecordNotFound for a job that no longer needs processing.
func (d streamsDao) ResolveAudioVOD(ctx context.Context, job model.AudioJob) (source AudioSource, err error) {
	err = d.withAudioSource(ctx, job, func(tx *gorm.DB, current AudioSource) error {
		var row model.AudioRendition
		if err := pendingAudio(tx, job, current).Select("stream_id").Take(&row).Error; err != nil {
			return err
		}
		source = current
		return nil
	})
	return source, err
}

func (d streamsDao) FinishAudioVOD(ctx context.Context, job model.AudioJob, state model.AudioState, playlistURL string) error {
	updates := map[string]any{"state": state}
	switch {
	case state == model.AudioReady && playlistURL != "", state == model.AudioAbsent && playlistURL == "":
		updates["playlist_url"] = playlistURL
	case state == model.AudioFailed && playlistURL == "":
		// Keep any published rendition of this video.
	default:
		return fmt.Errorf("invalid VOD audio result %q", state)
	}
	err := d.withAudioSource(ctx, job, func(tx *gorm.DB, source AudioSource) error {
		return pendingAudio(tx, job, source).Updates(updates).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil // acknowledge a completion for a deleted or retired recording
	}
	return err
}

// SaveVODSource publishes one version of the video and retires its audio jobs
// together. Older runners omit revision, so their reports always invalidate.
func (d streamsDao) SaveVODSource(ctx context.Context, id uint, version model.StreamVersion, playlistURL, revision string, privateByDefault bool) error {
	col, ok := vodPlaylistColumn[version]
	if !ok || id == 0 || playlistURL == "" {
		return errors.New("invalid VOD source")
	}
	defer Cache.Clear()
	// A copy can finish audio after the revision check. MariaDB must lock its
	// latest row when retiring it, even if the check saw an earlier snapshot.
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var stream model.Stream
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "recording").First(&stream, id).Error; err != nil {
			return err
		}
		key := audioSourceKey(playlistURL)
		// A copied lecture can replace this rendition without locking this stream.
		current, err := lockRendition(tx, id, version, key)
		if err != nil {
			return err
		}
		if revision != "" {
			var retired int64
			if err := tx.Model(&model.RetiredVODRevision{}).
				Where("stream_id = ? AND version = ? AND revision = ?", id, version, revision).
				Count(&retired).Error; err != nil {
				return err
			}
			if retired != 0 {
				return ErrRetiredVODRevision
			}
		}
		changes := map[string]any{col: playlistURL, "recording": true}
		if !stream.Recording && privateByDefault {
			changes["private"] = true
		}
		if err := tx.Model(&stream).Updates(changes).Error; err != nil {
			return err
		}
		if !bytes.Equal(current.SourceKey, key) {
			if err := renditionFor(tx, id, version).Update("source_key", key).Error; err != nil {
				return err
			}
		} else if revision != "" && current.SourceRevision == revision {
			return nil
		}
		// Copies still pointing to the overwritten file also lost their audio.
		// Copies pointing to the old URL of a moved source still have their video.
		if err := retireVODRevisions(tx, key, revision); err != nil {
			return err
		}
		return tx.Model(&model.AudioRendition{}).Where("source_key = ?", key).
			Updates(map[string]any{"generation": "", "state": "", "playlist_url": "", "source_revision": revision}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func retireVODRevisions(tx *gorm.DB, key []byte, replacement string) error {
	var retired []model.RetiredVODRevision
	err := tx.Model(&model.AudioRendition{}).Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("stream_id, version, source_revision AS revision").
		Where("source_key = ? AND source_revision <> '' AND source_revision <> ?", key, replacement).
		Find(&retired).Error
	if err != nil || len(retired) == 0 {
		return err
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&retired, 100).Error
}
