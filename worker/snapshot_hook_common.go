package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// common definition for snapshot hooks

// an ideal structure might look like the following:
// [fsdir]/serve/[mirror_name] -> a soft link to ../snapshot/[mirror_name]/@[unix_timestamp]
// [fsdir]/working/[mirror_name] -> a rw subvolume created from ../snapshot/[mirror_name]/@[unix_timestamp]
// [fsdir]/snapshot/[mirror_name]/@[unix_timestamp] -> the latest ro snapshot / subvolume
type providerSnapshotConfig struct {
	// global mirror serve dir
	mirrorDir string
	// web server serve dir, a soft link to mirrorServeDir
	mirrorWebServeDir string
	// a soft link to the latest snapshot (ideally ro)
	mirrorServeDir string
	// created subvolume (ideally rw) for syncing, and should only exist when syncing
	// case: succeeded => create a new ro snapshot (@[timestamp]), update the soft link and delete the previous one
	// case: failed => ~~delete the rw subvolume~~ keep it to resume progress
	mirrorWorkingDir string
	// store all (ideally ro) snapshots
	mirrorSnapshotDir string

	uid int
	gid int
}

func (c *providerSnapshotConfig) NewSnapshotName() string {
	return fmt.Sprintf("@%d", time.Now().Unix())
}

// create dir and chown to defined user/group
func (c *providerSnapshotConfig) tryCreateAndChownDir(path string) {
	fullPath, err := filepath.Abs(path)
	if err != nil {
		logger.Errorf("failed to get abs path %s: %s", path, err.Error())
	}
	if _, err = os.Stat(fullPath); os.IsNotExist(err) {
		err := os.MkdirAll(fullPath, 0755)
		if err != nil {
			logger.Errorf("failed to create dir %s: %s", fullPath, err.Error())
		}
		if err := os.Chown(fullPath, c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", fullPath, err.Error())
		}
	}
}

// create (modify) symlink newname to point to oldname
//
// newname is a symbolic link to oldname
func (c *providerSnapshotConfig) tryLink(oldname, newname string) {
	rfi, err := os.Lstat(newname)
	if err != nil {
		if os.IsNotExist(err) {
			// create if not exist
			if err := os.Symlink(oldname, newname); err != nil {
				logger.Errorf("failed to create symlink %s: %s", newname, err.Error())
			}
			return
		}
		logger.Errorf("failed to lstat %s: %s", newname, err.Error())
		return
	}
	if rfi.Mode()&os.ModeSymlink != 0 {
		pointsToPath, err := os.Readlink(newname)
		if err != nil {
			logger.Errorf("failed to read symlink %s: %s", newname, err.Error())
			return
		}

		if pointsToPath == newname {
			// already points to correct location
			return
		}
	}
	if err := os.Remove(newname); err != nil {
		logger.Errorf("failed to remove %s: %s", newname, err.Error())
		return
	}

	if err := os.Symlink(oldname, newname); err != nil {
		logger.Errorf("failed to re-create symlink %s: %s", newname, err.Error())
		return
	}
}
