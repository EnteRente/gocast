package actions

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"uuid"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tum-dev/gocast/runner/config"
	"github.com/tum-dev/gocast/runner/pkg/metrics"
	"github.com/tum-dev/gocast/runner/protobuf"
)

// GenerateAudioVOD waits for a processing slot before resolving the source. A
// signed URL obtained at dispatch could expire while other recordings are processed.
func GenerateAudioVOD(job *protobuf.AudioVODJob, resolve func(context.Context, *protobuf.AudioVODJob) (string, error)) Action {
	return func(ctx context.Context, _ *slog.Logger, notify chan *protobuf.Notification, _ map[string]any, _ *metrics.Broker) error {
		release, err := acquirePostProcessing(ctx)
		if err != nil {
			return AbortingError(err)
		}
		defer release()

		source, err := resolve(ctx, job)
		if err == nil && source == "" {
			return nil // the manager has already retired this generation
		}
		var playlist string
		if err == nil {
			playlist, err = makeAudioPlaylist(ctx, job, source)
		}
		if ctx.Err() != nil {
			return AbortingError(ctx.Err())
		}
		result := &protobuf.AudioVODFinishedNotification{Job: job}
		switch {
		case err != nil:
			result.Result = &protobuf.AudioVODFinishedNotification_Failed{Failed: &emptypb.Empty{}}
		case playlist == "":
			result.Result = &protobuf.AudioVODFinishedNotification_NoAudio{NoAudio: &emptypb.Empty{}}
		default:
			result.Result = &protobuf.AudioVODFinishedNotification_PlaylistUrl{PlaylistUrl: playlist}
		}
		select {
		case notify <- &protobuf.Notification{Data: &protobuf.Notification_AudioVodFinished{AudioVodFinished: result}}:
			return AbortingError(err)
		case <-ctx.Done():
			return AbortingError(ctx.Err())
		}
	}
}

// Each execution gets its own directory, including when two runners receive
// the same generation. Neither can overwrite the other's published media.
func makeAudioPlaylist(ctx context.Context, job *protobuf.AudioVODJob, source string) (string, error) {
	input, err := url.Parse(source)
	if err != nil || input.Host == "" || (input.Scheme != "http" && input.Scheme != "https") {
		return "", errors.New("the recording source needs an HTTP(S) URL")
	}
	edge, err := url.Parse(config.Config.EdgeServer)
	if err != nil || edge.Host == "" || (edge.Scheme != "http" && edge.Scheme != "https") {
		return "", errors.New("the edge server needs an HTTP(S) URL")
	}
	versionDir := path.Join(strconv.FormatUint(job.GetStreamId(), 10), job.GetVersion().String())
	if local := sharedVODPlaylist(*input, edge); local != "" {
		source = local
	}
	dir := path.Join(versionDir, "audio", uuid.NewV4().String())
	created, err := convertAudioVOD(ctx, source, filepath.Join(config.Config.StoragePath, filepath.FromSlash(dir)))
	if err != nil || !created {
		return "", err
	}
	return edge.JoinPath("vod", dir, "playlist.m3u8").String(), nil
}

// A copy can point at another lecture's recording. The edge's public address
// can be unreachable from the runner (localhost in Compose).
func sharedVODPlaylist(input url.URL, edge *url.URL) string {
	query, err := url.ParseQuery(input.RawQuery)
	if err != nil {
		return ""
	}
	query.Del("jwt")
	input.RawQuery = query.Encode()
	rel, ok := strings.CutPrefix(input.String(), edge.JoinPath("vod").String()+"/")
	if !ok {
		return ""
	}
	parts := strings.Split(rel, "/")
	if len(parts) != 3 || parts[2] != "playlist.m3u8" {
		return ""
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || id == 0 || protobuf.StreamVersion_value[parts[1]] == 0 {
		return ""
	}
	local := filepath.Join(config.Config.StoragePath, filepath.FromSlash(rel))
	if info, err := os.Stat(local); err == nil && info.Mode().IsRegular() {
		return local
	}
	return ""
}
