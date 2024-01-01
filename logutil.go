package cidataloader

import (
	"context"
	"github.com/sirupsen/logrus"
)

const (
	buildIDKey  = "buildId"
	jobKey      = "job"
	filenameKey = "filename"
)

func logwithctx(ctx context.Context) *logrus.Entry {

	logger := logrus.NewEntry(logrus.New())

	if ctx != nil {
		if buildId := ctx.Value(buildIDKey); buildId != nil {
			logger = logger.WithField(buildIDKey, buildId)
		}

		if jobId := ctx.Value(jobKey); jobId != nil {
			logger = logger.WithField(jobKey, jobId)
		}

		if filename := ctx.Value(filenameKey); filename != nil {
			logger = logger.WithField(filenameKey, filename)
		}
	}

	return logger
}

func addlogctx(ctx context.Context, jobBuildId, jobId, jobFilename string) context.Context {
	logCtx := context.WithValue(ctx, buildIDKey, jobBuildId)
	logCtx = context.WithValue(logCtx, jobKey, jobId)
	logCtx = context.WithValue(logCtx, filenameKey, jobFilename)

	return logCtx
}
