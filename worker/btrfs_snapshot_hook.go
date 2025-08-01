//go:build linux
// +build linux

package worker

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/dennwc/btrfs"
)

type btrfsSnapshotHook struct {
	provider mirrorProvider
	config   providerBtrfsSnapshotConfig
}

type providerBtrfsSnapshotConfig = providerSnapshotConfig

// initialize and create dir / subvolume if needed
func newProviderBtrfsSnapshotConfig(mirrorDir string, snsConfig snapshotConfig, mirror mirrorConfig, uid, gid int) *providerBtrfsSnapshotConfig {
	fsPath := snsConfig.BtrfsTypeConfig.FsPath
	c := &providerBtrfsSnapshotConfig{
		mirrorDir:         mirrorDir,
		mirrorServeDir:    filepath.Join(fsPath, snsConfig.ServePrefix, mirror.Dir),
		mirrorWorkingDir:  filepath.Join(fsPath, snsConfig.WorkingPrefix, mirror.Dir),
		mirrorSnapshotDir: filepath.Join(fsPath, snsConfig.SnapshotPrefix, mirror.Dir),

		uid: uid,
		gid: gid,
	}

	// create [mirror_dir]/[mirror_name]
	realServeDir := filepath.Join(mirrorDir, mirror.Name)
	c.tryCreateAndChownDir(realServeDir)

	// create [btrfs]/snapshot/[mirror_name]
	c.tryCreateAndChownDir(c.mirrorSnapshotDir)

	// create [btrfs]/working
	c.tryCreateAndChownDir(path.Dir(c.mirrorWorkingDir))

	// create [btrfs]/serve
	c.tryCreateAndChownDir(path.Dir(c.mirrorServeDir))

	// link [mirror_dir]/[mirror_name] -> [btrfs]/serve
	if _, err := os.Stat(realServeDir); os.IsNotExist(err) {
		relativePath, err := filepath.Rel(realServeDir, c.mirrorServeDir)
		if err != nil {
			logger.Errorf("failed to get relative path %s: %s", realServeDir, err.Error())
		}
		if err := os.Symlink(relativePath, realServeDir); err != nil {
			logger.Errorf("failed to create symlink %s: %s", realServeDir, err.Error())
		}
	}

	if _, err := c.LatestBtrfsSnapshot(); err != nil {
		logger.Errorf("failed to get latest Btrfs snapshot for: %s", mirror.Name, err.Error())
	}

	return c
}

func (c *providerBtrfsSnapshotConfig) tryCreateBtrfsSubvolume(path string) error {
	err := btrfs.CreateSubVolume(path)
	if err != nil {
		logger.Errorf("failed to create Btrfs subvolume %s: %s", path, err.Error())
	} else {
		logger.Noticef("created new Btrfs subvolume %s", path)
	}

	return err
}

func (c *providerBtrfsSnapshotConfig) tryCreateBtrfsSnapshot(from, to string, ro bool) error {
	err := btrfs.SnapshotSubVolume(from, to, ro)
	if err != nil {
		logger.Errorf("failed to create Btrfs snapshot %s from %s: %s", to, from, err.Error())
	} else {
		logger.Noticef("created new Btrfs snapshot %s from %s", to, from)
	}

	return err
}

func (c *providerBtrfsSnapshotConfig) tryDeleteBtrfsSubvolume(path string) error {
	err := btrfs.DeleteSubVolume(path)
	if err != nil {
		logger.Errorf("failed to delete Btrfs subvolume %s: %s", path, err.Error())
	} else {
		logger.Noticef("deleted Btrfs subvolume %s", path)
	}

	return err
}

func (c *providerBtrfsSnapshotConfig) LatestBtrfsSnapshot() (string, error) {
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
		// create [btrfs]/snapshot/[mirror_name]/base subvolume if not exist
		if _, err := os.Stat(filepath.Join(c.mirrorSnapshotDir, "base")); os.IsNotExist(err) {
			if err := c.tryCreateBtrfsSubvolume(filepath.Join(c.mirrorSnapshotDir, "base")); err != nil {
				return "", fmt.Errorf("failed to create Btrfs subvolume: %s", err.Error())
			}
		} else {
			if is, err := btrfs.IsSubVolume(filepath.Join(c.mirrorSnapshotDir, "base")); err != nil {
				return "", fmt.Errorf("failed to check if %s is a Btrfs subvolume: %s", filepath.Join(c.mirrorSnapshotDir, "base"), err.Error())
			} else if !is {
				return "", fmt.Errorf("%s is not a Btrfs subvolume", filepath.Join(c.mirrorSnapshotDir, "base"))
			}
		}

		snapshotName := c.NewSnapshotName()
		snapshotDir := filepath.Join(c.mirrorSnapshotDir, snapshotName)
		err := c.tryCreateBtrfsSnapshot(filepath.Join(c.mirrorSnapshotDir, "base"), snapshotDir, true)
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

func (c *providerBtrfsSnapshotConfig) LatestBtrfsSnapshotPath() (string, error) {
	snapshotName, err := c.LatestBtrfsSnapshot()
	if err != nil {
		return "", err
	}
	return filepath.Join(c.mirrorSnapshotDir, snapshotName), nil
}

// the user who runs the jobs (typically `tunasync`) should be granted the permission to run btrfs commands
func newBtrfsSnapshotHook(provider mirrorProvider, config providerBtrfsSnapshotConfig) *btrfsSnapshotHook {
	return &btrfsSnapshotHook{
		provider: provider,
		config:   config,
	}
}

// create a new rw Btrfs working snapshot before first attempt
func (h *btrfsSnapshotHook) preJob() error {
	workingDir := h.config.mirrorWorkingDir
	latestSnapshot, err := h.config.LatestBtrfsSnapshotPath()
	if err != nil {
		return err
	}

	if _, err := os.Stat(workingDir); err == nil {
		logger.Noticef("Btrfs working snapshot %s exists, resuming", workingDir)
	} else if os.IsNotExist(err) {
		// create rw temp snapshot
		if err := h.config.tryCreateBtrfsSnapshot(latestSnapshot, workingDir, false); err != nil {
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

func (h *btrfsSnapshotHook) preExec() error {
	return nil
}

func (h *btrfsSnapshotHook) postExec() error {
	return nil
}

// create a new ro snapshot from the working snapshot
// update the symlink to the latest snapshot
// and delete all old snapshots if exists
func (h *btrfsSnapshotHook) postSuccess() error {
	workingDir := h.config.mirrorWorkingDir
	newSnapshot := h.config.NewSnapshotName()
	newSnapshotPath := filepath.Join(h.config.mirrorSnapshotDir, newSnapshot)
	relativePath, err := filepath.Rel(filepath.Dir(h.config.mirrorServeDir), newSnapshotPath)
	if err != nil {
		return err
	}

	// create ro snapshot
	err = h.config.tryCreateBtrfsSnapshot(workingDir, newSnapshotPath, true)
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
	logger.Noticef("updated symlink %s", h.config.mirrorServeDir)

	// delete working snapshot
	if err := h.config.tryDeleteBtrfsSubvolume(workingDir); err != nil {
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
				if is, err := btrfs.IsSubVolume(snapshotDir); err != nil {
					logger.Errorf("failed to check if %s is a Btrfs subvolume: %s", snapshotDir, err.Error())
				} else if !is {
					logger.Errorf("%s is not a Btrfs subvolume", snapshotDir)
				} else {
					logger.Noticef("deleting old Btrfs snapshot %s", snapshotDir)
					_ = h.config.tryDeleteBtrfsSubvolume(snapshotDir)
				}
			}
		}
	}

	return nil
}

func (h *btrfsSnapshotHook) postFail() error {
	// workingDir := h.config.mirrorWorkingDir
	//
	// return tryDeleteSubvolume(workingDir)

	// Just keep the working snapshot to resume progress, rsync will handle it
	return nil
}
