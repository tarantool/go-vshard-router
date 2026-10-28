package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type tarantoolLogHandler struct {
	loggerf LogfProvider
	// ctx is the router's context. go-tarantool logs through the non-context
	// slog methods, so the record's own ctx is context.Background() and this is
	// the only source of caller values (trace id, tenant) on those records.
	ctx   context.Context
	attrs string
	group string
}

var _ slog.Handler = tarantoolLogHandler{}

func (h tarantoolLogHandler) Enabled(context.Context, slog.Level) bool {
	return true // The provider owns level filtering.
}

func (h tarantoolLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.loggerf == nil {
		return nil
	}

	var rendered strings.Builder
	rendered.WriteString(r.Message)
	rendered.WriteString(h.attrs)
	r.Attrs(func(attr slog.Attr) bool {
		appendLogAttr(&rendered, h.group, attr)
		return true
	})

	// Every level maps to the same one on the provider. go-tarantool reports
	// connect, reconnect and reopen failures at Warn, and a canceled request's
	// late response as Warn too, so collapsing Warn into Errorf would make a
	// routine reconnect or a successful bucket lookup indistinguishable from a
	// real failure.
	logf := h.loggerf.Debugf
	switch {
	case r.Level >= slog.LevelError:
		logf = h.loggerf.Errorf
	case r.Level >= slog.LevelWarn:
		logf = h.loggerf.Warnf
	case r.Level >= slog.LevelInfo:
		logf = h.loggerf.Infof
	}

	//nolint:contextcheck // h.ctx is the router context captured at construction.
	logCtx := h.ctx
	if logCtx == nil {
		logCtx = ctx
	}
	logf(logCtx, "tarantool: %s", rendered.String())
	return nil
}

func (h tarantoolLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var rendered strings.Builder
	rendered.WriteString(h.attrs)
	for _, attr := range attrs {
		appendLogAttr(&rendered, h.group, attr)
	}
	h.attrs = rendered.String()
	return h
}

func (h tarantoolLogHandler) WithGroup(name string) slog.Handler {
	if name != "" {
		h.group += name + "."
	}
	return h
}

func appendLogAttr(rendered *strings.Builder, group string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			group += attr.Key + "."
		}
		for _, nested := range attr.Value.Group() {
			appendLogAttr(rendered, group, nested)
		}
		return
	}
	rendered.WriteByte(' ')
	rendered.WriteString(group)
	rendered.WriteString(attr.Key)
	rendered.WriteByte('=')
	if attr.Value.Kind() == slog.KindString || attr.Value.Kind() == slog.KindAny {
		fmt.Fprintf(rendered, "%q", attr.Value.String())
	} else {
		rendered.WriteString(attr.Value.String())
	}
}
