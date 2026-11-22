package apiv2

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	protobuf "github.com/TUM-Dev/gocast/apiv2/protobuf/server"
	"github.com/TUM-Dev/gocast/dao"
	"github.com/TUM-Dev/gocast/mock_dao"
	"github.com/TUM-Dev/gocast/model"
	"github.com/TUM-Dev/gocast/pkg/runner_manager"
)

func TestGenerateAudioVOD(t *testing.T) {
	stream := model.Stream{Model: gorm.Model{ID: 37}, CourseID: 9}
	streams := mock_dao.NewMockStreamsDao(gomock.NewController(t))
	audit, entries := auditCapture(t)
	api := &API{dao: dao.DaoWrapper{StreamsDao: streams, AuditDao: audit}}
	admin := &model.User{Model: gorm.Model{ID: 2}, Role: model.AdminType}
	info := &grpc.UnaryServerInfo{FullMethod: protobuf.AdminService_GenerateAudioVOD_FullMethodName}
	call := func(ctx context.Context, source protobuf.WatchViewKind) error {
		req := &protobuf.GenerateAudioVODRequest{StreamId: uint32(stream.ID), Source: source}
		_, err := api.authorize(ctx, req, info, func(ctx context.Context, _ any) (any, error) {
			return api.GenerateAudioVOD(ctx, req)
		})
		return err
	}

	camera := protobuf.WatchViewKind_WATCH_VIEW_KIND_CAMERA
	require.Equal(t, codes.Unauthenticated, status.Code(call(t.Context(), camera)))
	for _, viewer := range []*model.User{{Role: model.StudentType}, {Role: model.LecturerType}} {
		require.Equal(t, codes.PermissionDenied, status.Code(call(asCaller(viewer), camera)))
	}
	for source, want := range map[protobuf.WatchViewKind]model.StreamVersion{
		camera: model.CAM,
		protobuf.WatchViewKind_WATCH_VIEW_KIND_COMBINED:     model.COMB,
		protobuf.WatchViewKind_WATCH_VIEW_KIND_PRESENTATION: model.PRES,
	} {
		streams.EXPECT().GetStreamByID(gomock.Any(), "37").Return(stream, nil)
		calls := 0
		WithAudioVODGenerator(func(_ context.Context, id uint, version model.StreamVersion) error {
			calls++
			require.Equal(t, stream.ID, id)
			require.Equal(t, want, version)
			return nil
		})(api)
		require.NoError(t, call(asCaller(admin), source))
		require.Equal(t, 1, calls)
	}
	require.Len(t, *entries, 3)
	for _, entry := range *entries {
		require.Equal(t, admin, entry.User)
		require.Equal(t, &stream.ID, entry.StreamID)
		require.Equal(t, &stream.CourseID, entry.CourseID)
	}
}

func TestGenerateAudioVODErrors(t *testing.T) {
	req := &protobuf.GenerateAudioVODRequest{StreamId: 37, Source: protobuf.WatchViewKind_WATCH_VIEW_KIND_CAMERA}
	streams := mock_dao.NewMockStreamsDao(gomock.NewController(t))
	api := &API{dao: dao.DaoWrapper{StreamsDao: streams}}
	check := func(req *protobuf.GenerateAudioVODRequest, want codes.Code) {
		t.Helper()
		_, err := api.GenerateAudioVOD(t.Context(), req)
		require.Equal(t, want, status.Code(err), "%v", err)
	}
	check(&protobuf.GenerateAudioVODRequest{Source: req.Source}, codes.InvalidArgument)
	check(&protobuf.GenerateAudioVODRequest{StreamId: req.StreamId}, codes.InvalidArgument)
	check(&protobuf.GenerateAudioVODRequest{StreamId: req.StreamId, Source: 99}, codes.InvalidArgument)
	check(req, codes.Unavailable)

	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("no stored playlist: %w", gorm.ErrRecordNotFound), codes.NotFound},
		{runner_manager.ErrNoRunner, codes.Unavailable},
		{status.Error(codes.Unimplemented, "old runner"), codes.Unavailable},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("storage failure"), codes.Unknown},
	} {
		streams.EXPECT().GetStreamByID(gomock.Any(), "37").Return(model.Stream{Model: gorm.Model{ID: 37}}, nil)
		api.requestAudioVOD = func(context.Context, uint, model.StreamVersion) error { return tc.err }
		check(req, tc.code)
	}
	streams.EXPECT().GetStreamByID(gomock.Any(), "37").Return(model.Stream{}, gorm.ErrRecordNotFound)
	check(req, codes.NotFound)
}
