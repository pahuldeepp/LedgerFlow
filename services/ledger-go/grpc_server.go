package main

import (
	"net"

	adminv1 "ledger-go/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)
func startGRPCServer(addr string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	s := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(s, newAdminServer())

	go func() {
		L().Info("grpc_started", zap.String("addr", addr))
		if err := s.Serve(lis); err != nil {
			L().Fatal("grpc_serve_error", zap.Error(err))
		}
	}()

	return s, lis, nil
}