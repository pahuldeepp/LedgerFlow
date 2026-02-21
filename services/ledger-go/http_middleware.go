package main

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const requestIDKey = "request_id"

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-Id")
		if rid == "" {
			rid = uuid.NewString()
		}
		c.Set(requestIDKey, rid)
		c.Header("X-Request-Id", rid)
		c.Next()
	}
}

func ZapLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		lat := time.Since(start)
		method := c.Request.Method
		path := c.FullPath()
		status := strconv.Itoa(c.Writer.Status())

		if path == "" {
			path = "unknown"
		}

		// ✅ Correct metric usage
		ObserveHTTPRequest(method, path, status, lat)

		rid, _ := c.Get(requestIDKey)

		L().Info("http_request",
			zap.Any("request_id", rid),
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", lat),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
		)
	}
}

func ZapRecovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		rid, _ := c.Get(requestIDKey)

		L().Error("panic",
			zap.Any("request_id", rid),
			zap.Any("recovered", recovered),
		)

		c.AbortWithStatus(500)
	})
}
