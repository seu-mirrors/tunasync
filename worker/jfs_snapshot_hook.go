package worker

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

type jfsSnapshotHook struct {
	provider mirrorProvider
	config   providerJfsSnapshotConfig
}

type providerJfsSnapshotConfig = providerSnapshotConfig

// initialize and create dir / subvolume if needed
func newproviderJfsSnapshotConfig(mirrorDir string, snsConfig snapshotConfig, mirror mirrorConfig, uid, gid int) *providerJfsSnapshotConfig {
	fsPath := snsConfig.JfsTypeConfig.FsPath
	c := &providerJfsSnapshotConfig{
		mirrorDir:         mirrorDir,
		mirrorWebServeDir: filepath.Join(mirrorDir, mirror.Name),
		mirrorServeDir:    filepath.Join(fsPath, snsConfig.ServePrefix, mirror.Dir),
		mirrorWorkingDir:  filepath.Join(fsPath, snsConfig.WorkingPrefix, mirror.Dir),
		mirrorSnapshotDir: filepath.Join(fsPath, snsConfig.SnapshotPrefix, mirror.Dir),

		uid: uid,
		gid: gid,
	}

	// create [mirror_dir]/[mirror_name]
	c.tryCreateAndChownDir(c.mirrorWebServeDir)

	// create [jfs]/snapshot/[mirror_name]
	c.tryCreateAndChownDir(c.mirrorSnapshotDir)

	// create [jfs]/working
	c.tryCreateAndChownDir(path.Dir(c.mirrorWorkingDir))

	// create [jfs]/serve
	c.tryCreateAndChownDir(path.Dir(c.mirrorServeDir))

	if _, err := c.LatestJfsSnapshot(); err != nil {
		logger.Errorf("failed to get latest jfs snapshot for: %s", mirror.Name, err.Error())
	}

	return c
}

func (c *providerJfsSnapshotConfig) tryCreateJfsSnapshot(from, to string) error {
	// append slash to make sure we are copying dir
	cmd := exec.Command("juicefs", "clone", "-p", from, to)
	err := cmd.Run()
	if err != nil {
		logger.Errorf("failed to create jfs snapshot %s from %s: %s", to, from, err.Error())
	} else {
		logger.Noticef("created new jfs snapshot %s from %s", to, from)
	}

	return err
}

func (c *providerJfsSnapshotConfig) tryDeleteJfsSnapshot(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		logger.Errorf("failed to delete jfs snapshot %s: %s", path, err.Error())
	} else {
		logger.Noticef("deleted jfs snapshot %s", path)
	}

	return err
}

func (c *providerJfsSnapshotConfig) LatestJfsSnapshot() (string, error) {
	snapshotEntries, err := os.ReadDir(c.mirrorSnapshotDir)
	snapshots := make([]string, 0)
	if err != nil {
		logger.Errorf("failed to read dir %s: %s", c.mirrorSnapshotDir, err.Error())
	} else {
		for _, entry := range snapshotEntries {
			if entry.Name()[0] == '@' {
				snapshots = append(snapshots, entry.Name())
			}
		}
	}

	if len(snapshots) == 0 {
		// create [jfs]/snapshot/[mirror_name]/base dir if not exist
		baseDir := filepath.Join(c.mirrorSnapshotDir, "base")
		c.tryCreateAndChownDir(baseDir)

		snapshotName := c.NewSnapshotName()
		snapshotDir := filepath.Join(c.mirrorSnapshotDir, snapshotName)
		err := c.tryCreateJfsSnapshot(baseDir, snapshotDir)
		return snapshotName, err
	}

	sort.Slice(snapshots, func(i, j int) bool {
		// Extract timestamps without the '@' prefix
		timestamp1, _ := strconv.ParseInt(snapshots[i][1:], 10, 64)
		timestamp2, _ := strconv.ParseInt(snapshots[j][1:], 10, 64)

		// Compare timestamps in descending order
		return timestamp1 > timestamp2
	})

	return snapshots[0], nil
}

func (c *providerSnapshotConfig) LatestJfsSnapshotPath() (string, error) {
	snapshotName, err := c.LatestJfsSnapshot()
	if err != nil {
		return "", err
	}
	return filepath.Join(c.mirrorSnapshotDir, snapshotName), nil
}

// the user who runs the jobs (typically `tunasync`) should be granted the permission to run juicefs commands
func newJfsSnapshotHook(provider mirrorProvider, config providerJfsSnapshotConfig) *jfsSnapshotHook {
	return &jfsSnapshotHook{
		provider: provider,
		config:   config,
	}
}

// create a new rw jfs working snapshot before first attempt
func (h *jfsSnapshotHook) preJob() error {
	workingDir := h.config.mirrorWorkingDir
	latestSnapshot, err := h.config.LatestJfsSnapshotPath()
	if err != nil {
		return err
	}

	if _, err := os.Stat(workingDir); err == nil {
		logger.Noticef("jfs working snapshot %s exists, resuming", workingDir)
	} else if os.IsNotExist(err) {
		// create temp snapshot
		if err := h.config.tryCreateJfsSnapshot(latestSnapshot, workingDir); err != nil {
			return err
		}
		if err := os.Chown(workingDir, h.config.uid, h.config.gid); err != nil {
			logger.Warningf("failed to chown %s to %d:%d: %s", workingDir, h.config.uid, h.config.gid, err.Error())
		}
	} else {
		return err
	}

	return nil
}

func (h *jfsSnapshotHook) preExec() error {
	return nil
}

func (h *jfsSnapshotHook) postExec() error {
	return nil
}

// create a new ro snapshot from the working snapshot
// update the symlink to the latest snapshot
// and delete all old snapshots if exists
func (h *jfsSnapshotHook) postSuccess() error {
	workingDir := h.config.mirrorWorkingDir
	newSnapshot := h.config.NewSnapshotName()
	newSnapshotPath := filepath.Join(h.config.mirrorSnapshotDir, newSnapshot)
	relativePath, err := filepath.Rel(filepath.Dir(h.config.mirrorServeDir), newSnapshotPath)
	if err != nil {
		return err
	}

	// create snapshot
	err = h.config.tryCreateJfsSnapshot(workingDir, newSnapshotPath)
	if err != nil {
		return err
	}

	// update symlink
	if err := os.Remove(h.config.mirrorServeDir + ".tmp"); err != nil && !os.IsNotExist(err) {
		logger.Errorf("failed to remove symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	if err := os.Symlink(relativePath, h.config.mirrorServeDir+".tmp"); err != nil {
		logger.Errorf("failed to create symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	if err := os.Rename(h.config.mirrorServeDir+".tmp", h.config.mirrorServeDir); err != nil {
		logger.Errorf("failed to rename symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	// link [mirror_dir]/[mirror_name] -> [jfs]/serve/[mirror_name]
	relativePath, err = filepath.Rel(h.config.mirrorWebServeDir, h.config.mirrorServeDir)
	if err != nil {
		return err
	}
	h.config.tryLink(relativePath, h.config.mirrorWebServeDir)
	logger.Noticef("updated symlink %s", h.config.mirrorServeDir)

	// delete working snapshot
	if err := h.config.tryDeleteJfsSnapshot(workingDir); err != nil {
		return err
	}

	// delete old snapshots
	snapshotEntries, err := os.ReadDir(h.config.mirrorSnapshotDir)
	if err != nil {
		logger.Errorf("failed to read dir %s: %s", h.config.mirrorSnapshotDir, err.Error())
	} else {
		for _, entry := range snapshotEntries {
			if (entry.Name()[0] == '@' && entry.Name() != filepath.Base(newSnapshotPath)) || entry.Name() == "base" {
				snapshotDir := filepath.Join(h.config.mirrorSnapshotDir, entry.Name())
				logger.Noticef("deleting old jfs snapshot %s", snapshotDir)
				_ = h.config.tryDeleteJfsSnapshot(snapshotDir)
			}
		}
	}

	return nil
}

func (h *jfsSnapshotHook) postFail() error {
	// workingDir := h.config.mirrorWorkingDir
	//
	// return tryDeleteSubvolume(workingDir)

	// Just keep the working snapshot to resume progress, rsync will handle it
	return nil
}
