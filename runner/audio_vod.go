package runner

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tum-dev/gocast/runner/pkg/actions"
	"github.com/tum-dev/gocast/runner/pkg/ptr"
	"github.com/tum-dev/gocast/runner/protobuf"
)

func (r *Runner) GenerateAudioVOD(ctx context.Context, job *protobuf.AudioVODJob) (*protobuf.GenerateAudioVODResponse, error) {
	if job.GetStreamId() == 0 || job.GetGeneration() == "" {
		return nil, status.Error(codes.InvalidArgument, "stream ID and audio generation are required")
	}
	version, ok := protobuf.StreamVersion_name[int32(job.GetVersion())]
	if !ok || job.GetVersion() == protobuf.StreamVersion_STREAM_VERSION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "invalid stream version")
	}
	data := map[string]any{"streamID": job.GetStreamId(), "streamVersion": version}
	logger := r.log.With("stream_id", job.GetStreamId(), "stream_version", version, "generation", job.GetGeneration())
	action := actions.GenerateAudioVOD(job, r.resolveAudioVODSource)
	id := r.RunActionContext(ctx, []actions.Action{action}, nil, data, logger)
	return &protobuf.GenerateAudioVODResponse{JobId: ptr.Take(id)}, nil
}

func (r *Runner) resolveAudioVODSource(ctx context.Context, job *protobuf.AudioVODJob) (string, error) {
	client, err := r.getManagerClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	source, err := client.ResolveAudioVODSource(ctx, job)
	if shouldReconnect(err) {
		r.invalidateManagerConn(client)
	}
	return source.GetPlaylistUrl(), err
}
