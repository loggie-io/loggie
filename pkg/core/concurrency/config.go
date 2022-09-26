package concurrency

type Config struct {
	Enable    *bool     `yaml:"enable,omitempty" default:"false" validate:"required"`
	Goroutine Goroutine `yaml:"goroutine,omitempty"  validate:"dive"`
	Rtt       Rtt       `yaml:"rtt,omitempty"  validate:"dive"`
	Ratio     Ratio     `yaml:"ratio,omitempty"  validate:"dive"`
	Duration  Duration  `yaml:"duration,omitempty"  validate:"dive"`
}

func (c *Config) Validate() error {
	return nil
}

func (c *Config) Merge(from *Config) {
	if from == nil {
		return
	}

	if c.Enable != from.Enable {
		return
	}

	c.Goroutine.Merge(&from.Goroutine)
	c.Rtt.Merge(&from.Rtt)
	c.Ratio.Merge(&from.Ratio)
	c.Duration.Merge(&from.Duration)

}

type Goroutine struct {
	InitThreshold    int     `yaml:"initThreshold,omitempty" default:"16" validate:"required,gte=1"`
	MaxGoroutine     int     `yaml:"maxGoroutine,omitempty" default:"30" validate:"required,gte=1"`
	UnstableTolerate int     `yaml:"unstableTolerate,omitempty" default:"3" validate:"required,gte=1"`
	ChannelLenOfCap  float64 `yaml:"channelLenOfCap,omitempty" default:"0.4" validate:"required,gt=0"`
}

func (g *Goroutine) Validate() error {
	return nil
}

func (g *Goroutine) Merge(from *Goroutine) {
	if from == nil {
		return
	}
	if g.InitThreshold == 0 {
		g.InitThreshold = from.InitThreshold
	}

	if g.MaxGoroutine == 0 {
		g.MaxGoroutine = from.MaxGoroutine
	}

	if g.UnstableTolerate == 0 {
		g.UnstableTolerate = from.UnstableTolerate
	}

	if g.ChannelLenOfCap == 0 {
		g.ChannelLenOfCap = from.ChannelLenOfCap
	}
}

type Rtt struct {
	BlockJudgeThreshold float64 `yaml:"blockJudgeThreshold,omitempty" default:"1.1" validate:"required,gt=1"`
	NewRttWeigh         float64 `yaml:"newRttWeigh,omitempty" default:"0.5" validate:"required,gte=0,lte=1"`
}

func (r *Rtt) Validate() error {
	return nil
}

func (r *Rtt) Merge(from *Rtt) {
	if from == nil {
		return
	}

	if r.BlockJudgeThreshold == 0 {
		r.BlockJudgeThreshold = from.BlockJudgeThreshold
	}

	if r.NewRttWeigh == 0 {
		r.NewRttWeigh = from.NewRttWeigh
	}
}

type Ratio struct {
	Multi             int `yaml:"multi,omitempty" default:"2" validate:"required,gt=1"`
	Linear            int `yaml:"linear,omitempty" default:"2" validate:"required,gt=1"`
	LinearWhenBlocked int `yaml:"linearWhenBlocked,omitempty" default:"4" validate:"required,gt=1"`
}

func (r *Ratio) Validate() error {
	return nil
}

func (r *Ratio) Merge(from *Ratio) {
	if from == nil {
		return
	}

	if r.Multi == 0 {
		r.Multi = from.Multi
	}

	if r.Linear == 0 {
		r.Linear = from.Linear
	}

	if r.LinearWhenBlocked == 0 {
		r.LinearWhenBlocked = from.LinearWhenBlocked
	}
}

type Duration struct {
	Unstable int `yaml:"unstable,omitempty" default:"15" validate:"required,gte=1"`
	Stable   int `yaml:"stable,omitempty" default:"30" validate:"required,gte=1"`
}

func (d *Duration) Validate() error {
	return nil
}

func (d *Duration) Merge(from *Duration) {
	if from == nil {
		return
	}

	if d.Unstable == 0 {
		d.Unstable = from.Unstable
	}

	if d.Stable == 0 {
		d.Stable = from.Stable
	}
}
