package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func New(model, policy string) (*Authorizer, error) {
	e, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, err
	}
	return &Authorizer{enforcer: e}, nil
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if enforced, err := a.enforcer.Enforce(subject, object, action); err != nil {
		return err
	} else if !enforced {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, object, action)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
