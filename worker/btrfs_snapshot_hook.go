//go:build linux
// +build linux

package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/dennwc/btrfs"
)

type btrfsSnapshotHook struct {
	provider mirrorProvider
	config   providerBtrfsSnapshotConfig
}

// an ideal structure might look like the following:
// /mnt/btrfs/tunasync/serve/[mirror_name] -> a soft link to ../snapshot/[mirror_name]/@[unix_timestamp]
// /mnt/btrfs/tunasync/working/[mirror_name] -> a rw subvolume created from ../snapshot/[mirror_name]/@[unix_timestamp]
// /mnt/btrfs/tunasync/snapshot/[mirror_name]/@[unix_timestamp] -> the latest ro snapshot / subvolume
type providerBtrfsSnapshotConfig struct {
	// a soft link to the latest ro snapshot
	mirrorServeDir string
	// created rw subvolume for syncing, and should only exist when syncing
	// case: succeeded => create a new ro snapshot (@[timestamp]), update the soft link and delete the previous one
	// case: failed => delete the rw subvolume
	mirrorWorkingDir string
	// store all ro snapshots
	mirrorSnapshotDir string

	uid int
	gid int
}

// initialize and create dir / subvolume if needed
func newProviderBtrfsSnapshotConfig(mirrorDir string, btrfsConfig btrfsSnapshotConfig, mirror mirrorConfig, uid, gid int) *providerBtrfsSnapshotConfig {
	c := &providerBtrfsSnapshotConfig{
		mirrorServeDir:    filepath.Join(mirrorDir, btrfsConfig.ServePrefix, mirror.Dir),
		mirrorWorkingDir:  filepath.Join(mirrorDir, btrfsConfig.WorkingPrefix, mirror.Dir),
		mirrorSnapshotDir: filepath.Join(mirrorDir, btrfsConfig.SnapshotPrefix, mirror.Dir),

		uid: uid,
		gid: gid,
	}

	// create [btrfs]/snapshot/[mirror_name]
	if _, err := os.Stat(c.mirrorSnapshotDir); os.IsNotExist(err) {
		err := os.MkdirAll(c.mirrorSnapshotDir, 0755)
		if err != nil {
			logger.Errorf("failed to create dir %s: %s", c.mirrorSnapshotDir, err.Error())
		}
		if err := os.Chown(c.mirrorSnapshotDir, c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", c.mirrorSnapshotDir, err.Error())
		}
		if err := os.Chown(filepath.Dir(c.mirrorSnapshotDir), c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", filepath.Dir(c.mirrorSnapshotDir), err.Error())
		}
	}

	// create [btrfs]/working
	if _, err := os.Stat(filepath.Dir(c.mirrorWorkingDir)); os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(c.mirrorWorkingDir), 0755)
		if err != nil {
			logger.Errorf("failed to create dir %s: %s", filepath.Dir(c.mirrorWorkingDir), err.Error())
		}
		if err := os.Chown(filepath.Dir(c.mirrorWorkingDir), c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", filepath.Dir(c.mirrorWorkingDir), err.Error())
		}
	}

	// create [btrfs]/serve
	if _, err := os.Stat(filepath.Dir(c.mirrorServeDir)); os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(c.mirrorServeDir), 0755)
		if err != nil {
			logger.Errorf("failed to create dir %s: %s", filepath.Dir(c.mirrorServeDir), err.Error())
		}
		if err := os.Chown(filepath.Dir(c.mirrorServeDir), c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", filepath.Dir(c.mirrorServeDir), err.Error())
		}
	}

	// create [btrfs]/snapshot/[mirror_name]/base subvolume if not exist
	if _, err := os.Stat(filepath.Join(c.mirrorSnapshotDir, "base")); os.IsNotExist(err) {
		err := btrfs.CreateSubVolume(filepath.Join(c.mirrorSnapshotDir, "base"))
		if err != nil {
			logger.Errorf("failed to create Btrfs subvolume %s: %s", filepath.Join(c.mirrorSnapshotDir, "base"), err.Error())
		}
		logger.Noticef("created new Btrfs subvolume %s", filepath.Join(c.mirrorSnapshotDir, "base"))
	} else {
		if is, err := btrfs.IsSubVolume(filepath.Join(c.mirrorSnapshotDir, "base")); err != nil {
			logger.Errorf("failed to check if %s is a Btrfs subvolume: %s", filepath.Join(c.mirrorSnapshotDir, "base"), err.Error())
		} else if !is {
			logger.Errorf("%s is not a Btrfs subvolume", filepath.Join(c.mirrorSnapshotDir, "base"))
		}
	}

	if _, err := c.LatestSnapshot(); err != nil {
		logger.Errorf("failed to get latest Btrfs snapshot for: %s", mirror.Name, err.Error())
	}

	return c
}

func (c *providerBtrfsSnapshotConfig) LatestSnapshot() (string, error) {
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
		snapshotName := c.NewSnapshotName()
		snapshotDir := filepath.Join(c.mirrorSnapshotDir, snapshotName)
		err := btrfs.SnapshotSubVolume(filepath.Join(c.mirrorSnapshotDir, "base"), snapshotDir, true)
		if err != nil {
			logger.Errorf("failed to create Btrfs snapshot %s: %s", snapshotDir, err.Error())
		}
		logger.Noticef("created new Btrfs snapshot %s", snapshotDir)
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

func (c *providerBtrfsSnapshotConfig) LatestSnapshotPath() (string, error) {
	snapshotName, err := c.LatestSnapshot()
	if err != nil {
		return "", err
	}
	return filepath.Join(c.mirrorSnapshotDir, snapshotName), nil
}

func (c *providerBtrfsSnapshotConfig) NewSnapshotName() string {
	return fmt.Sprintf("@%d", time.Now().Unix())
}

// the user who runs the jobs (typically `tunasync`) should be granted the permission to run btrfs commands
func newBtrfsSnapshotHook(provider mirrorProvider, config providerBtrfsSnapshotConfig) *btrfsSnapshotHook {
	return &btrfsSnapshotHook{
		provider: provider,
		config:   config,
	}
}

func (h *btrfsSnapshotHook) preJob() error {
	return nil
}

// create a new rw Btrfs working snapshot for every sync attempt
func (h *btrfsSnapshotHook) preExec() error {
	workingDir := h.config.mirrorWorkingDir
	latestSnapshot, err := h.config.LatestSnapshotPath()
	if err != nil {
		return err
	}

	if _, err := os.Stat(workingDir); err == nil {
		if is, err := btrfs.IsSubVolume(workingDir); err != nil {
			return err
		} else if !is {
			return fmt.Errorf("workingDir %s exists but isn't a Btrfs subvolume", workingDir)
		} else {
			logger.Noticef("Btrfs working snapshot %s exists, removing", workingDir)
			if err := btrfs.DeleteSubVolume(workingDir); err != nil {
				logger.Errorf("failed to delete Btrfs working snapshot %s: %s", workingDir, err.Error())
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	// create rw temp snapshot
	err = btrfs.SnapshotSubVolume(latestSnapshot, workingDir, false)
	if err != nil {
		logger.Errorf("failed to create Btrfs working snapshot %s: %s", workingDir, err.Error())
		return err
	}
	logger.Noticef("created new Btrfs working snapshot %s", workingDir)

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

	// create ro snapshot
	err := btrfs.SnapshotSubVolume(workingDir, newSnapshotPath, true)
	if err != nil {
		logger.Errorf("failed to create new Btrfs ro snapshot %s: %s", newSnapshotPath, err.Error())
		return err
	}
	logger.Noticef("created new Btrfs ro snapshot %s", newSnapshotPath)

	// update symlink
	if err := os.Remove(h.config.mirrorServeDir + ".tmp"); err != nil && !os.IsNotExist(err) {
		logger.Errorf("failed to remove symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	if err := os.Symlink(newSnapshotPath, h.config.mirrorServeDir+".tmp"); err != nil {
		logger.Errorf("failed to create symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	if err := os.Rename(h.config.mirrorServeDir+".tmp", h.config.mirrorServeDir); err != nil {
		logger.Errorf("failed to rename symlink %s: %s", h.config.mirrorServeDir+".tmp", err.Error())
		return err
	}
	logger.Noticef("updated symlink %s", h.config.mirrorServeDir)

	// delete working snapshot
	if err := btrfs.DeleteSubVolume(workingDir); err != nil {
		logger.Errorf("failed to delete Btrfs working snapshot %s: %s", workingDir, err.Error())
		return err
	}

	// delete old snapshots
	snapshotEntries, err := os.ReadDir(h.config.mirrorSnapshotDir)
	if err != nil {
		logger.Errorf("failed to read dir %s: %s", h.config.mirrorSnapshotDir, err.Error())
	} else {
		for _, entry := range snapshotEntries {
			if entry.Name()[0] == '@' && entry.Name() != filepath.Base(newSnapshotPath) {
				snapshotDir := filepath.Join(h.config.mirrorSnapshotDir, entry.Name())
				if is, err := btrfs.IsSubVolume(snapshotDir); err != nil {
					logger.Errorf("failed to check if %s is a Btrfs subvolume: %s", snapshotDir, err.Error())
				} else if !is {
					logger.Errorf("%s is not a Btrfs subvolume", snapshotDir)
				} else {
					logger.Noticef("deleting old Btrfs snapshot %s", snapshotDir)
					if err := btrfs.DeleteSubVolume(snapshotDir); err != nil {
						logger.Errorf("failed to delete Btrfs snapshot %s: %s", snapshotDir, err.Error())
					}
				}
			}
		}
	}

	return nil
}

// delete working snapshot
func (h *btrfsSnapshotHook) postFail() error {
	workingDir := h.config.mirrorWorkingDir
	err := btrfs.DeleteSubVolume(workingDir)
	if err != nil {
		logger.Errorf("failed to delete Btrfs working snapshot %s: %s", workingDir, err.Error())
	}

	return err
}
