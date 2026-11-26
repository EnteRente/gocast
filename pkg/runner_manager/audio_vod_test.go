package runner_manager

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"
	"uuid"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/gorm"

	"github.com/tum-dev/gocast/runner/pkg/ptr"
	"github.com/tum-dev/gocast/runner/protobuf"

	"github.com/TUM-Dev/gocast/dao"
	"github.com/TUM-Dev/gocast/mock_dao"
	"github.com/TUM-Dev/gocast/model"
	"github.com/TUM-Dev/gocast/tools"
)

type audioRunner struct {
	protobuf.UnimplementedRunnerServiceServer
	result error
	jobs   chan *protobuf.AudioVODJob
}

func (r *audioRunner) GenerateAudioVOD(_ context.Context, job *protobuf.AudioVODJob) (*protobuf.GenerateAudioVODResponse, error) {
	r.jobs <- job
	return &protobuf.GenerateAudioVODResponse{JobId: ptr.Take("runner-job")}, r.result
}

func TestRequestAudioVOD(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
	}{
		{"accepted", codes.OK},
		{"older runner rejected the job", codes.Unimplemented},
		{"reply lost after acceptance", codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			r := &audioRunner{result: status.Error(tc.code, tc.name), jobs: make(chan *protobuf.AudioVODJob, 1)}
			srv := grpc.NewServer()
			protobuf.RegisterRunnerServiceServer(srv, r)
			go func() { _ = srv.Serve(l) }()
			t.Cleanup(srv.Stop)
			ctrl := gomock.NewController(t)
			runners := mock_dao.NewMockRunnerDao(ctrl)
			runners.EXPECT().ReserveRunner(gomock.Any()).Return(model.Runner{
				Hostname: "127.0.0.1", Port: uint32(l.Addr().(*net.TCPAddr).Port),
			}, nil)
			streams := mock_dao.NewMockStreamsDao(ctrl)
			var started model.AudioJob
			streams.EXPECT().BeginAudioVOD(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, job model.AudioJob) error {
				started = job
				return nil
			})
			if tc.code == codes.Unimplemented {
				streams.EXPECT().FinishAudioVOD(gomock.Any(), gomock.Any(), model.AudioFailed, "").DoAndReturn(
					func(_ context.Context, job model.AudioJob, _ model.AudioState, _ string) error {
						require.Equal(t, started, job)
						return nil
					})
			}
			m := New(dao.DaoWrapper{RunnerDao: runners, StreamsDao: streams})
			err = m.RequestAudioVOD(t.Context(), 8, model.PRES)
			require.Equal(t, tc.code, status.Code(err))
			dispatched := <-r.jobs
			require.Equal(t, uint64(8), dispatched.GetStreamId())
			require.Equal(t, protobuf.StreamVersion_STREAM_VERSION_PRESENTATION, dispatched.GetVersion())
			_, err = uuid.Parse(dispatched.GetGeneration())
			require.NoError(t, err)
			require.Equal(t, model.AudioJob{StreamID: 8, Version: model.PRES, Generation: dispatched.GetGeneration()}, started)
		})
	}
}

func TestResolveAudioVODSource(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	t.Cleanup(tools.SetJWTKeyForTest(key))
	const edge = "https://edge.example"
	const archive = "https://archive.example"
	previous := tools.Cfg.VodURLTemplate
	tools.Cfg.VodURLTemplate = archive + "/vod/%s.mp4/playlist.m3u8"
	t.Cleanup(func() { tools.Cfg.VodURLTemplate = previous })
	req := &protobuf.AudioVODJob{
		StreamId: ptr.Take(uint64(8)), Version: protobuf.StreamVersion_STREAM_VERSION_CAMERA.Enum(), Generation: ptr.Take("current"),
	}
	job := model.AudioJob{StreamID: 8, Version: model.CAM, Generation: "current"}
	for _, tc := range []struct {
		name, source, runnerEdge string
		sign                     bool
	}{
		{"runner edge", edge + "/vod/3/old/playlist.m3u8?lang=en&jwt=expired", edge, true},
		{"older runner using the archive edge", archive + "/vod/a.mp4/playlist.m3u8?lang=en", "", true},
		{"external archive", "https://external.example/vod/a.mp4/playlist.m3u8?sig=x%2fy&lang=en", edge, false},
		{"different port", edge + ":8443/vod/3/old/playlist.m3u8", edge, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req.EdgeServer = nil
			if tc.runnerEdge != "" {
				req.EdgeServer = ptr.Take(tc.runnerEdge)
			}
			streams := mock_dao.NewMockStreamsDao(gomock.NewController(t))
			streams.EXPECT().ResolveAudioVOD(gomock.Any(), job).Return(dao.AudioSource{CourseID: 6, PlaylistURL: tc.source}, nil)
			got, err := New(dao.DaoWrapper{StreamsDao: streams}).ResolveAudioVODSource(t.Context(), req)
			require.NoError(t, err)
			if !tc.sign {
				require.Equal(t, tc.source, got.GetPlaylistUrl())
				return
			}
			u, err := url.Parse(got.GetPlaylistUrl())
			require.NoError(t, err)
			q := u.Query()
			require.Equal(t, "en", q.Get("lang"))
			claims := &tools.JWTPlaylistClaims{}
			_, err = jwt.ParseWithClaims(q.Get("jwt"), claims, func(*jwt.Token) (any, error) { return &key.PublicKey, nil })
			require.NoError(t, err)
			u.RawQuery = ""
			require.Equal(t, u.String(), claims.Playlist)
			require.Equal(t, "8", claims.StreamID)
			require.Equal(t, "6", claims.CourseID)
			require.False(t, claims.Download)
			require.WithinDuration(t, time.Now().Add(7*time.Hour), claims.ExpiresAt.Time, time.Minute)
		})
	}
	streams := mock_dao.NewMockStreamsDao(gomock.NewController(t))
	m := New(dao.DaoWrapper{StreamsDao: streams})
	streams.EXPECT().ResolveAudioVOD(gomock.Any(), job).Return(dao.AudioSource{}, gorm.ErrRecordNotFound)
	got, err := m.ResolveAudioVODSource(t.Context(), req)
	require.NoError(t, err)
	require.Empty(t, got.GetPlaylistUrl())
	streams.EXPECT().ResolveAudioVOD(gomock.Any(), job).Return(dao.AudioSource{}, errors.New("unavailable database"))
	_, err = m.ResolveAudioVODSource(t.Context(), req)
	require.Equal(t, codes.Internal, status.Code(err))

	tools.Cfg.VodURLTemplate = "https://stream.lrz.de/vod/%s.mp4/playlist.m3u8"
	const legacy = "https://stream.lrz.de/vod/lecture.mp4/playlist.m3u8?jwt=archive-issued"
	raw, err := audioSourceURL(dao.AudioSource{PlaylistURL: legacy}, 8, edge)
	require.NoError(t, err)
	require.Equal(t, legacy, raw)
}

func TestAudioVODCompletion(t *testing.T) {
	version := protobuf.StreamVersion_STREAM_VERSION_COMBINED
	job := &protobuf.AudioVODJob{StreamId: ptr.Take(uint64(8)), Version: version.Enum(), Generation: ptr.Take("current")}
	modelJob := model.AudioJob{StreamID: 8, Version: model.COMB, Generation: "current"}
	const output = "https://edge/vod/8/STREAM_VERSION_COMBINED/audio/68dcec04-1478-494d-8b1b-b657716eb996/playlist.m3u8"
	streams := mock_dao.NewMockStreamsDao(gomock.NewController(t))
	m := New(dao.DaoWrapper{StreamsDao: streams})
	notify := func(result *protobuf.AudioVODFinishedNotification) error {
		t.Helper()
		result.Job = job
		_, err := m.Notify(t.Context(), &protobuf.Notification{Data: &protobuf.Notification_AudioVodFinished{AudioVodFinished: result}})
		return err
	}
	streams.EXPECT().FinishAudioVOD(gomock.Any(), modelJob, model.AudioReady, output).Return(nil)
	require.NoError(t, notify(&protobuf.AudioVODFinishedNotification{Result: &protobuf.AudioVODFinishedNotification_PlaylistUrl{PlaylistUrl: output}}))
	streams.EXPECT().FinishAudioVOD(gomock.Any(), modelJob, model.AudioAbsent, "").Return(nil)
	require.NoError(t, notify(&protobuf.AudioVODFinishedNotification{Result: &protobuf.AudioVODFinishedNotification_NoAudio{NoAudio: &emptypb.Empty{}}}))
	streams.EXPECT().FinishAudioVOD(gomock.Any(), modelJob, model.AudioFailed, "").Return(nil)
	require.NoError(t, notify(&protobuf.AudioVODFinishedNotification{Result: &protobuf.AudioVODFinishedNotification_Failed{Failed: &emptypb.Empty{}}}))
	require.Equal(t, codes.InvalidArgument, status.Code(notify(&protobuf.AudioVODFinishedNotification{})))
	require.Equal(t, codes.InvalidArgument, status.Code(notify(&protobuf.AudioVODFinishedNotification{Result: &protobuf.AudioVODFinishedNotification_PlaylistUrl{PlaylistUrl: output + "?jwt=temporary"}})))
	job.Version = protobuf.StreamVersion_STREAM_VERSION_CAMERA.Enum()
	require.Equal(t, codes.InvalidArgument, status.Code(notify(&protobuf.AudioVODFinishedNotification{Result: &protobuf.AudioVODFinishedNotification_PlaylistUrl{PlaylistUrl: output}})))
}
