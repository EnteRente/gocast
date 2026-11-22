package actions

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tum-dev/gocast/runner/config"
	"github.com/tum-dev/gocast/runner/pkg/ptr"
	"github.com/tum-dev/gocast/runner/protobuf"
)

func TestGenerateAudioVOD(t *testing.T) {
	requireAudioTools(t)
	job := &protobuf.AudioVODJob{StreamId: ptr.Take(uint64(9)), Version: protobuf.StreamVersion_STREAM_VERSION_CAMERA.Enum(), Generation: ptr.Take("first")}
	const versionDir = "9/STREAM_VERSION_CAMERA"
	storage := t.TempDir()
	files := http.StripPrefix("/vod/", http.FileServer(http.Dir(storage)))
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/audio/") {
			http.Error(w, "source only available on the shared volume", http.StatusForbidden)
			return
		}
		files.ServeHTTP(w, r)
	}))
	t.Cleanup(edge.Close)
	oldStorage, oldEdge := config.Config.StoragePath, config.Config.EdgeServer
	config.Config.StoragePath, config.Config.EdgeServer = storage, edge.URL
	t.Cleanup(func() { config.Config.StoragePath, config.Config.EdgeServer = oldStorage, oldEdge })

	local := makeAudioVODInput(t, "aac", "2")
	if err := os.MkdirAll(filepath.Join(storage, "9"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Dir(local), filepath.Join(storage, versionDir)); err != nil {
		t.Fatal(err)
	}
	copySource := "4/STREAM_VERSION_PRESENTATION"
	if err := os.MkdirAll(filepath.Join(storage, "4"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Dir(makeAudioVODInput(t, "aac", "3")), filepath.Join(storage, copySource)); err != nil {
		t.Fatal(err)
	}
	remote := serveAudioVOD(t, makeAudioVODInput(t, "aac", "4"), nil)
	silent := serveAudioVOD(t, makeAudioVODInput(t, "", "2"), nil)
	for _, tc := range []struct {
		name, input string
		duration    float64
	}{
		{"shared volume", edge.URL + "/vod/" + versionDir + "/playlist.m3u8?jwt=test", 2},
		{"copied lecture", edge.URL + "/vod/" + copySource + "/playlist.m3u8?jwt=test", 3},
		{"remote", remote + "/playlist.m3u8", 4},
		{"silent", silent + "/playlist.m3u8", 0},
		{"unreadable", remote + "/missing.m3u8", 0},
		{"superseded", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action := GenerateAudioVOD(job, func(context.Context, *protobuf.AudioVODJob) (string, error) { return tc.input, nil })
			notify := make(chan *protobuf.Notification, 1)
			err := action(audioTestContext(t), testLogger(), notify, nil, nil)
			if tc.name == "superseded" {
				if err != nil || len(notify) != 0 {
					t.Fatalf("retired job: %v, notifications: %d", err, len(notify))
				}
				return
			}
			if len(notify) != 1 {
				t.Fatalf("missing completion: %v", err)
			}
			result := (<-notify).GetAudioVodFinished()
			if result.GetJob() != job {
				t.Fatalf("unexpected job in notification: %v", result)
			}
			switch tc.name {
			case "silent":
				if err != nil || result.GetNoAudio() == nil {
					t.Fatalf("silent source: %v, %v", result, err)
				}
			case "unreadable":
				if !IsAbortingError(err) || result.GetFailed() == nil {
					t.Fatalf("unreadable source: %v, %v", result, err)
				}
			default:
				prefix := edge.URL + "/vod/" + versionDir + "/audio/"
				if err != nil || !strings.HasPrefix(result.GetPlaylistUrl(), prefix) {
					t.Fatalf("unexpected audio URL: %q, %v", result.GetPlaylistUrl(), err)
				}
				rel := strings.TrimPrefix(result.GetPlaylistUrl(), edge.URL+"/vod/")
				assertAudioVOD(t, filepath.Dir(filepath.Join(storage, rel)), 1, tc.duration)
				decodeAudioTestMedia(t, result.GetPlaylistUrl())
			}
		})
	}
}

func TestGenerateAudioVODCancelledInQueue(t *testing.T) {
	old := postProcessingSlots
	postProcessingSlots = make(chan struct{}, 1)
	t.Cleanup(func() { postProcessingSlots = old })
	postProcessingSlots <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	action := GenerateAudioVOD(nil, func(context.Context, *protobuf.AudioVODJob) (string, error) {
		t.Fatal("resolved the source while all processing slots were taken")
		return "", nil
	})
	notify := make(chan *protobuf.Notification, 1)
	if err := action(ctx, testLogger(), notify, nil, nil); !errors.Is(err, context.Canceled) || len(notify) != 0 {
		t.Fatalf("cancelled job: %v, notifications: %d", err, len(notify))
	}
}
