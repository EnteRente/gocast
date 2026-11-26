package apiv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	e "github.com/TUM-Dev/gocast/apiv2/errors"
	protobuf "github.com/TUM-Dev/gocast/apiv2/protobuf/server"
	"github.com/TUM-Dev/gocast/model"
	"github.com/TUM-Dev/gocast/pkg/runner_manager"
)

func WithAudioVODGenerator(request func(context.Context, uint, model.StreamVersion) error) Option {
	return func(a *API) { a.requestAudioVOD = request }
}

func (a *API) GenerateAudioVOD(ctx context.Context, req *protobuf.GenerateAudioVODRequest) (*emptypb.Empty, error) {
	if req.GetStreamId() == 0 {
		return nil, e.WithStatus(http.StatusBadRequest, errors.New("a stream ID is required"))
	}
	version, ok := recordingVersions[req.GetSource()]
	if !ok {
		return nil, e.WithStatus(http.StatusBadRequest, errors.New("choose a combined, camera, or presentation source"))
	}
	if a.requestAudioVOD == nil {
		return nil, e.WithStatus(http.StatusServiceUnavailable, runner_manager.ErrNoRunner)
	}
	stream, err := a.dao.GetStreamByID(ctx, strconv.FormatUint(uint64(req.GetStreamId()), 10))
	if err != nil {
		return nil, e.FromGorm(err, "can't find stream")
	}
	if err := a.requestAudioVOD(ctx, stream.ID, version); err != nil {
		switch {
		case errors.Is(err, runner_manager.ErrNoRunner), status.Code(err) == codes.Unavailable, status.Code(err) == codes.Unimplemented:
			return nil, e.WithStatus(http.StatusServiceUnavailable, errors.New("no runner is available to generate VOD audio"))
		case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
			return nil, e.WithStatus(http.StatusGatewayTimeout, errors.New("timed out requesting VOD audio"))
		default:
			return nil, e.FromGorm(err, "can't find the recorded source")
		}
	}
	a.audit(ctx, model.AuditContent, fmt.Sprintf("Requested VOD audio for %s", version), forStream(stream))
	return &emptypb.Empty{}, nil
}
