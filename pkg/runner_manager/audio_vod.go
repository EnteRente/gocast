package runner_manager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/tum-dev/gocast/runner/pkg/ptr"
	"github.com/tum-dev/gocast/runner/protobuf"

	"github.com/TUM-Dev/gocast/dao"
	"github.com/TUM-Dev/gocast/model"
	"github.com/TUM-Dev/gocast/tools"
)

// RequestAudioVOD queues one source of an existing recording. The runner resolves
// its playlist after a processing slot opens, which can be hours after dispatch.
func (m *Manager) RequestAudioVOD(ctx context.Context, streamID uint, version model.StreamVersion) (err error) {
	if streamID == 0 || (version != model.COMB && version != model.CAM && version != model.PRES) {
		return status.Error(codes.InvalidArgument, "a stream ID and recording source are required")
	}
	v := protobufStreamVersion(version)
	ctx, span := startSpan(withStreamTrace(ctx, uint64(streamID), v), "manager.request_audio_vod", streamAttrs(uint64(streamID), v)...)
	defer endSpan(span, &err)
	ctx, cancel := context.WithTimeout(ctx, runnerDispatchTimeout)
	defer cancel()
	runner, client, conn, err := m.getClient(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	job := model.AudioJob{StreamID: streamID, Version: version, Generation: uuid.NewV4().String()}
	if err := m.dao.StreamsDao.BeginAudioVOD(ctx, job); err != nil {
		return fmt.Errorf("begin VOD audio job: %w", err)
	}
	resp, err := client.GenerateAudioVOD(ctx, &protobuf.AudioVODJob{
		StreamId: ptr.Take(uint64(streamID)), Version: v.Enum(), Generation: ptr.Take(job.Generation),
	})
	if err != nil {
		// A lost reply can leave a job running. Only a definite rejection may retire it.
		switch status.Code(err) {
		case codes.Unimplemented, codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.ResourceExhausted:
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), runnerDispatchTimeout)
			defer cancel()
			if saveErr := m.dao.StreamsDao.FinishAudioVOD(cleanup, job, model.AudioFailed, ""); saveErr != nil {
				return errors.Join(err, fmt.Errorf("record rejected VOD audio job: %w", saveErr))
			}
		}
		return fmt.Errorf("dispatch VOD audio job: %w", err)
	}
	m.logger.Info("requested VOD audio", "stream", streamID, "version", version, "generation", job.Generation,
		"runner", runner.Hostname, "job", resp.GetJobId())
	return nil
}

func audioJob(req *protobuf.AudioVODJob) (model.AudioJob, error) {
	id, generation := req.GetStreamId(), req.GetGeneration()
	if id == 0 || uint64(uint(id)) != id || generation == "" || len(generation) > 36 {
		return model.AudioJob{}, status.Error(codes.InvalidArgument, "invalid VOD audio job")
	}
	switch req.GetVersion() {
	case protobuf.StreamVersion_STREAM_VERSION_COMBINED, protobuf.StreamVersion_STREAM_VERSION_CAMERA, protobuf.StreamVersion_STREAM_VERSION_PRESENTATION:
		return model.AudioJob{StreamID: uint(id), Version: modelStreamVersion(req.GetVersion()), Generation: generation}, nil
	default:
		return model.AudioJob{}, status.Error(codes.InvalidArgument, "invalid recording source")
	}
}

func (m *Manager) ResolveAudioVODSource(ctx context.Context, req *protobuf.AudioVODJob) (*protobuf.AudioVODSource, error) {
	job, err := audioJob(req)
	if err != nil {
		return nil, err
	}
	source, err := m.dao.StreamsDao.ResolveAudioVOD(ctx, job)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &protobuf.AudioVODSource{}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve VOD audio source: %v", err)
	}
	playlist, err := audioSourceURL(source, job.StreamID, req.GetEdgeServer())
	if err != nil {
		return nil, err
	}
	return &protobuf.AudioVODSource{PlaylistUrl: ptr.Take(playlist)}, nil
}

func audioSourceURL(source dao.AudioSource, streamID uint, runnerEdge string) (string, error) {
	u, err := url.Parse(source.PlaylistURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", status.Error(codes.FailedPrecondition, "the stored recording needs an HTTP(S) URL")
	}
	sameOrigin := func(raw string) bool {
		edge, err := url.Parse(raw)
		return err == nil && edge.Scheme == u.Scheme && strings.EqualFold(edge.Host, u.Host)
	}
	host := strings.ToLower(u.Hostname())
	lrz := host == "lrz.de" || strings.HasSuffix(host, ".lrz.de")
	archive := strings.ReplaceAll(tools.Cfg.VodURLTemplate, "%s", "_")
	if !sameOrigin(runnerEdge) && (lrz || !sameOrigin(archive)) {
		return source.PlaylistURL, nil // external archives supply their own access parameters
	}
	if u.User != nil || !strings.HasPrefix(u.Path, "/vod/") || path.Dir(u.Path) == "/vod" || path.Clean(u.Path) != u.Path {
		return "", status.Error(codes.FailedPrecondition, "the stored edge playlist needs its own VOD directory")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", status.Error(codes.FailedPrecondition, "invalid stored playlist query")
	}
	u.RawQuery, u.Fragment, u.ForceQuery = "", "", false
	claims := tools.JWTPlaylistClaims{
		Playlist: u.String(),
		StreamID: strconv.FormatUint(uint64(streamID), 10),
		CourseID: strconv.FormatUint(uint64(source.CourseID), 10),
	}
	token, err := claims.Sign()
	if err != nil {
		return "", status.Errorf(codes.Internal, "sign VOD audio source: %v", err)
	}
	query.Set("jwt", token)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (m *Manager) finishAudioVOD(ctx context.Context, result *protobuf.AudioVODFinishedNotification) error {
	job, err := audioJob(result.GetJob())
	if err != nil {
		return err
	}
	var state model.AudioState
	playlist := result.GetPlaylistUrl()
	switch {
	case result.GetNoAudio() != nil:
		state = model.AudioAbsent
	case result.GetFailed() != nil:
		state = model.AudioFailed
	case validAudioPlaylist(playlist, result.GetJob()):
		state = model.AudioReady
	default:
		return status.Error(codes.InvalidArgument, "invalid VOD audio result")
	}
	if err := m.dao.StreamsDao.FinishAudioVOD(ctx, job, state, playlist); err != nil {
		return status.Errorf(codes.Internal, "save VOD audio result: %v", err)
	}
	return nil
}

func validAudioPlaylist(raw string, job *protobuf.AudioVODJob) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	dir := path.Base(path.Dir(u.Path))
	if _, err := uuid.Parse(dir); err != nil {
		return false
	}
	suffix := path.Join("vod", strconv.FormatUint(job.GetStreamId(), 10), job.GetVersion().String(), "audio", dir, "playlist.m3u8")
	return path.Clean(u.Path) == u.Path && strings.HasSuffix(u.EscapedPath(), "/"+suffix)
}
