package internal

import (
	"encoding/json"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStatus(t *testing.T) {
	Convey("status json ser-de should work", t, func() {
		tz := "Asia/Tokyo"
		loc, err := time.LoadLocation(tz)
		So(err, ShouldBeNil)
		t := time.Date(2016, time.April, 16, 23, 8, 10, 0, loc)
		m := WebMirrorStatus{
			Name:          "tunalinux",
			Status:        Success,
			LastUpdate:    textTime{t},
			LastUpdateTs:  stampTime{t},
			LastStarted:   textTime{t},
			LastStartedTs: stampTime{t},
			LastEnded:     textTime{t},
			LastEndedTs:   stampTime{t},
			Scheduled:     textTime{t},
			ScheduledTs:   stampTime{t},
			Size:          "5GB",
			Upstream:      "rsync://mirrors.tuna.tsinghua.edu.cn/tunalinux/",
		}

		b, err := json.Marshal(m)
		So(err, ShouldBeNil)
		//fmt.Println(string(b))
		var m2 WebMirrorStatus
		err = json.Unmarshal(b, &m2)
		So(err, ShouldBeNil)
		// fmt.Printf("%#v", m2)
		So(m2.Name, ShouldEqual, m.Name)
		So(m2.Status, ShouldEqual, m.Status)
		So(m2.LastUpdate.Unix(), ShouldEqual, m.LastUpdate.Unix())
		So(m2.LastUpdateTs.Unix(), ShouldEqual, m.LastUpdate.Unix())
		So(m2.LastUpdate.UnixNano(), ShouldEqual, m.LastUpdate.UnixNano())
		So(m2.LastUpdateTs.UnixNano(), ShouldEqual, m.LastUpdate.UnixNano())
		So(m2.LastStarted.Unix(), ShouldEqual, m.LastStarted.Unix())
		So(m2.LastStartedTs.Unix(), ShouldEqual, m.LastStarted.Unix())
		So(m2.LastStarted.UnixNano(), ShouldEqual, m.LastStarted.UnixNano())
		So(m2.LastStartedTs.UnixNano(), ShouldEqual, m.LastStarted.UnixNano())
		So(m2.LastEnded.Unix(), ShouldEqual, m.LastEnded.Unix())
		So(m2.LastEndedTs.Unix(), ShouldEqual, m.LastEnded.Unix())
		So(m2.LastEnded.UnixNano(), ShouldEqual, m.LastEnded.UnixNano())
		So(m2.LastEndedTs.UnixNano(), ShouldEqual, m.LastEnded.UnixNano())
		So(m2.Scheduled.Unix(), ShouldEqual, m.Scheduled.Unix())
		So(m2.ScheduledTs.Unix(), ShouldEqual, m.Scheduled.Unix())
		So(m2.Scheduled.UnixNano(), ShouldEqual, m.Scheduled.UnixNano())
		So(m2.ScheduledTs.UnixNano(), ShouldEqual, m.Scheduled.UnixNano())
		So(m2.Size, ShouldEqual, m.Size)
		So(m2.Upstream, ShouldEqual, m.Upstream)
	})
	Convey("BuildWebMirrorStatus should work", t, func() {
		m := MirrorStatus{
			Name:        "arch-sync3",
			Worker:      "testWorker",
			IsMaster:    true,
			Status:      Failed,
			LastUpdate:  time.Now().Add(-time.Minute * 30),
			LastStarted: time.Now().Add(-time.Minute * 1),
			LastEnded:   time.Now(),
			Scheduled:   time.Now().Add(time.Minute * 5),
			Upstream:    "mirrors.tuna.tsinghua.edu.cn",
			Size:        "4GB",
		}

		var m2 WebMirrorStatus = BuildWebMirrorStatus(m)
		// fmt.Printf("%#v", m2)
		So(m2.Name, ShouldEqual, m.Name)
		So(m2.Status, ShouldEqual, m.Status)
		So(m2.LastUpdate.Unix(), ShouldEqual, m.LastUpdate.Unix())
		So(m2.LastUpdateTs.Unix(), ShouldEqual, m.LastUpdate.Unix())
		So(m2.LastUpdate.UnixNano(), ShouldEqual, m.LastUpdate.UnixNano())
		So(m2.LastUpdateTs.UnixNano(), ShouldEqual, m.LastUpdate.UnixNano())
		So(m2.LastStarted.Unix(), ShouldEqual, m.LastStarted.Unix())
		So(m2.LastStartedTs.Unix(), ShouldEqual, m.LastStarted.Unix())
		So(m2.LastStarted.UnixNano(), ShouldEqual, m.LastStarted.UnixNano())
		So(m2.LastStartedTs.UnixNano(), ShouldEqual, m.LastStarted.UnixNano())
		So(m2.LastEnded.Unix(), ShouldEqual, m.LastEnded.Unix())
		So(m2.LastEndedTs.Unix(), ShouldEqual, m.LastEnded.Unix())
		So(m2.LastEnded.UnixNano(), ShouldEqual, m.LastEnded.UnixNano())
		So(m2.LastEndedTs.UnixNano(), ShouldEqual, m.LastEnded.UnixNano())
		So(m2.Scheduled.Unix(), ShouldEqual, m.Scheduled.Unix())
		So(m2.ScheduledTs.Unix(), ShouldEqual, m.Scheduled.Unix())
		So(m2.Scheduled.UnixNano(), ShouldEqual, m.Scheduled.UnixNano())
		So(m2.ScheduledTs.UnixNano(), ShouldEqual, m.Scheduled.UnixNano())
		So(m2.Size, ShouldEqual, m.Size)
		So(m2.Upstream, ShouldEqual, m.Upstream)
	})
}
