package model

// AudioJob identifies one request. Retrying a recording uses a new generation
// so a late result from the interrupted job cannot win.
type AudioJob struct {
	StreamID   uint          `gorm:"primaryKey;autoIncrement:false"`
	Version    StreamVersion `gorm:"primaryKey;size:4"`
	Generation string        `gorm:"size:36"`
}

type AudioState string

const (
	AudioPending AudioState = "pending"
	AudioReady   AudioState = "ready"
	AudioAbsent  AudioState = "no_audio"
	AudioFailed  AudioState = "failed"
)

type AudioRendition struct {
	AudioJob `gorm:"embedded"`
	// A job's result belongs to the playlist URL it was requested for.
	SourceKey []byte     `gorm:"type:binary(32);not null"`
	State     AudioState `gorm:"size:8"`
	// The last successful output stays playable while the same video is retried.
	PlaylistURL string `gorm:"type:text"`
}
