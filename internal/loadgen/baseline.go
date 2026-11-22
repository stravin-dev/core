package loadgen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// RegisterBaselineScenarios registers two minimal smoke scenarios (one dev, one agent)
// so stravin-loadgen works standalone before full scenario packs are registered.
func RegisterBaselineScenarios(reg *Registry) error {
	if err := reg.Register(&baselineLsRemoteScenario{}); err != nil {
		return err
	}
	return reg.Register(&baselineCloneFsckScenario{})
}

type baselineLsRemoteScenario struct{}

func (s *baselineLsRemoteScenario) Name() string     { return "dev-baseline-ls-remote" }
func (s *baselineLsRemoteScenario) Persona() Persona { return PersonaDev }
func (s *baselineLsRemoteScenario) Weight() int      { return 10 }

func (s *baselineLsRemoteScenario) Run(ctx context.Context, env *ScenarioEnv) error {
	url := env.Config.RepoHTTPURL(env.Repo)
	return env.RunOp(ctx, s.Name(), "ls-remote", ClassifyOptions{}, func(opCtx context.Context) error {
		_, err := env.Git.LsRemote(opCtx, url)
		return err
	})
}

type baselineCloneFsckScenario struct{}

func (s *baselineCloneFsckScenario) Name() string     { return "agent-baseline-clone-fsck" }
func (s *baselineCloneFsckScenario) Persona() Persona { return PersonaAgent }
func (s *baselineCloneFsckScenario) Weight() int      { return 10 }

func (s *baselineCloneFsckScenario) Run(ctx context.Context, env *ScenarioEnv) error {
	url := env.Config.RepoHTTPURL(env.Repo)
	cloneDir := filepath.Join(env.WorkDir, fmt.Sprintf("clone-%d", env.RNG.Uint64()))
	defer func() { _ = os.RemoveAll(cloneDir) }()

	if err := env.RunOp(ctx, s.Name(), "clone", ClassifyOptions{}, func(opCtx context.Context) error {
		return env.Git.Clone(opCtx, url, cloneDir)
	}); err != nil {
		return err
	}

	clonedGit := env.Git.WithWorkDir(cloneDir)
	return clonedGit.Fsck(ctx)
}
