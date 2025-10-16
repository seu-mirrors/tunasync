//go:build !linux
// +build !linux

package worker

type btrfsSnapshotHook struct {
}

type providerBtrfsSnapshotConfig = providerSnapshotConfig

func newProviderBtrfsSnapshotConfig(mirrorDir string, snsConfig snapshotConfig, mirror mirrorConfig, uid, gid int) *providerBtrfsSnapshotConfig {
	// No-op implementation for non-Linux systems
	return nil
}

func newBtrfsSnapshotHook(provider mirrorProvider, config providerBtrfsSnapshotConfig) *btrfsSnapshotHook {
	return &btrfsSnapshotHook{}
}

func (h *btrfsSnapshotHook) postExec() error {
	return nil
}

func (h *btrfsSnapshotHook) postFail() error {
	return nil
}

func (h *btrfsSnapshotHook) postSuccess() error {
	return nil
}

func (h *btrfsSnapshotHook) preExec() error {
	return nil
}

func (h *btrfsSnapshotHook) preJob() error {
	return nil
}
