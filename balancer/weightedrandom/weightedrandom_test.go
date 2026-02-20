package weightedrandom

import (
	"testing"

	"google.golang.org/grpc/balancer"
)

func TestBalancerRegistration(t *testing.T) {
	if balancer.Get(Name) == nil {
		t.Fatalf("balancer %q not registered", Name)
	}
}


