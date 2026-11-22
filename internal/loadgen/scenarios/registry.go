package scenarios

import (
	"fmt"

	"stravin.dev/core/internal/loadgen"
)

// All returns the full catalog of 22 scenarios (11 Developer + 11 AI Agent).
func All() []loadgen.Scenario {
	return []loadgen.Scenario{
		// Developer Persona (11)
		&devCloneAndRead{},
		&devFeatureBranchPR{},
		&devIncrementalThinPack{},
		&devRebaseAndForcePush{},
		&devConcurrentMainPushRetry{},
		&devTagAndRelease{},
		&devLargeBinaryAsset{},
		&devBranchCleanupCycle{},
		&devShallowCloneUnshallow{},
		&devSSHFullLifecycle{},
		&devNoopEmptyPushAndFetch{},

		// AI Agent Persona (11)
		&agentRapidCheckpointBurst{},
		&agentParallelBranchFanout{},
		&agentSharedBranchRace{},
		&agentDeepTreeRefactor{},
		&agentOrphanBranchArtifact{},
		&agentMergeCommitDiamond{},
		&agentCherryPickBackport{},
		&agentWasmHookAllowAndReject{},
		&agentHighChurnCompactionTrigger{},
		&agentProtocolSwitchHTTPSSH{},
		&agentCorruptRefNegativeTest{},
	}
}

// RegisterAll registers all 22 developer and AI agent scenarios into reg.
func RegisterAll(reg *loadgen.Registry) error {
	if reg == nil {
		return fmt.Errorf("scenarios: registry is required")
	}
	for _, s := range All() {
		if err := reg.Register(s); err != nil {
			return err
		}
	}
	return nil
}
