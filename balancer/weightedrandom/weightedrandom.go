package weightedrandom

import (
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/internal/wrr"
	"google.golang.org/grpc/resolver"
)

const Name = "weighted_random_experimental"

type weightKey struct{}

func SetWeight(addr resolver.Address, weight int64) resolver.Address {
	if addr.Attributes == nil {
		addr.Attributes = attributes.New(weightKey{}, weight)
		return addr
	}
	addr.Attributes = addr.Attributes.WithValue(weightKey{}, weight)
	return addr
}

func GetWeight(addr resolver.Address) int64 {
	v := addr.Attributes.Value(weightKey{})
	if w, ok := v.(int64); ok {
		return w
	}
	return 1
}

func init() {
	balancer.Register(base.NewBalancerBuilder(Name, &pickerBuilder{}, base.Config{HealthCheck: true}))
}

type pickerBuilder struct{}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	w := wrr.NewAlias()
	for sc, scInfo := range info.ReadySCs {
		w.Add(sc, GetWeight(scInfo.Address))
	}

	return &picker{wrr: w}
}

type picker struct {
	wrr wrr.WRR
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	item := p.wrr.Next()
	return balancer.PickResult{SubConn: item.(balancer.SubConn)}, nil
}
