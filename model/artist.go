package model

import "github.com/google/uuid"

type Artist struct {
	ID       uint64
	GID      uuid.UUID
	Name     string `boltholdIndex:"Name"`
	NameSort string
	Works    []uint64 `boltholdIndex:"Works"`
}
