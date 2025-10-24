package internal

const (
	AlphaChannel       = "alpha"
	BetaChannel        = "beta"
	EarlyAccessChannel = "early-access"
	StableChannel      = "stable"
	RockSolidChannel   = "rock-solid"
	LTSChannel         = "lts"
)

func GetAllDefaultReleaseChannels() []string {
	return []string{
		AlphaChannel,
		BetaChannel,
		EarlyAccessChannel,
		StableChannel,
		RockSolidChannel,
	}
}

var channelsMap = map[string]string{
	AlphaChannel:       "Alpha",
	BetaChannel:        "Beta",
	EarlyAccessChannel: "Early Access",
	StableChannel:      "Stable",
	RockSolidChannel:   "Rock Solid",
	LTSChannel:         "LTS",
}

func ChannelIsValid(c string) bool {
	_, ok := channelsMap[c]

	return ok
}
