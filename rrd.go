package rrd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type RRD struct {
	DS   []DS
	RRA  []RRA
	Step int
}

type DS struct {
	Name      string
	Type      string
	Heartbeat interface{}
	Min       interface{}
	Max       interface{}
}

type RRA struct {
	CF   string
	Xff  float64
	Step interface{}
	Row  interface{}
}

type Graph struct {
	Args       []string
	Colors     []GraphColor
	Fonts      []GraphFont
	Data       []GraphData
	Script     []GraphCommand
	ShowLegend bool
}

type GraphColor struct {
	Name  string
	Value string
}

type GraphFont struct {
	Name   string
	Size   int
	Family string
}

type GraphData struct {
	Type  string // DEF, CDEF, VDEF
	Name  string
	Value string
}

type GraphCommand struct {
	Name  string
	Value string
}

func UpdateRRD(filename string, length int, values ...interface{}) error {
	path := filename
	if path[0] != '/' {
		path = "./" + path
	}
	counters := []string{"N"}
	for i := 0; i < length; i++ {
		if len(values) > i {
			counters = append(counters, fmt.Sprint(values[i]))
		} else {
			counters = append(counters, "U")
		}
	}
	for i := 0; i < 10; i++ {
		cmd := exec.Command("rrdtool", "update", path, strings.Join(counters, ":"))
		b, err := cmd.CombinedOutput()
		if err != nil {
			if i == 9 {
				return fmt.Errorf("Unable to update RRD file %s. output:(%s) err (%s)", path, string(b), err)
			}
			if strings.Contains(string(b), "could not lock RRD") {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			return fmt.Errorf("Unable to update RRD file %s. output:(%s) err (%s)", path, string(b), err)
		} else {
			break
		}
	}
	return nil
}

func CreateRRD(filename string, rrd RRD) error {
	path := filename
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	args := []string{
		"create",
		path,
		"--step",
		fmt.Sprint(rrd.Step),
	}
	for _, v := range rrd.DS {
		if v.Max == 0 {
			args = append(args, fmt.Sprintf("DS:%s:%s:%v:%v:U", v.Name, v.Type, v.Heartbeat, v.Min))
		} else {
			args = append(args, fmt.Sprintf("DS:%s:%s:%v:%v:%v", v.Name, v.Type, v.Heartbeat, v.Min, v.Max))
		}
	}
	for _, v := range rrd.RRA {
		args = append(args, fmt.Sprintf("RRA:%s:%v:%v:%v", v.CF, v.Xff, v.Step, v.Row))
	}

	os.MkdirAll(filepath.Dir(path), 0744)
	cmd := exec.Command("rrdtool", args...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Unable to update RRD file %s. output:(%s) err (%s)", path, string(b), err)
	}
	return nil
}

func MakeChart(tmpl Graph, from *time.Time, to *time.Time, height int, width int, c map[string][]string, filename string) error {
	os.MkdirAll(filepath.Dir(filename), 0744)
	args := []string{"graph", filename}

	// Add Args
	args = append(args, tmpl.Args...)

	if from != nil {
		args = append(args, []string{"--start", fmt.Sprint(from.Unix())}...)
	}
	if to != nil {
		args = append(args, []string{"--end", fmt.Sprint(to.Unix())}...)
	}
	if !tmpl.ShowLegend {
		args = append(args, "--no-legend")
		args = append(args, "--only-graph")
	}
	args = append(args, []string{"--height", fmt.Sprint(height), "--width", fmt.Sprint(width), "--full-size-mode"}...)

	// Add Colors
	for _, color := range tmpl.Colors {
		args = append(args, "-c")
		args = append(args, color.Name+color.Value)
	}

	// Add Fonts
	for _, font := range tmpl.Fonts {
		args = append(args, "--font")
		args = append(args, fmt.Sprintf("%s:%d:%s", font.Name, font.Size, font.Family))
	}

	// Add Data
	var dynamicData bool
	for _, data := range tmpl.Data {
		dynamicData = false
		for k := range c {
			if strings.Contains(data.Name, k) || strings.Contains(data.Value, k) {
				dynamicData = true
				// Process dynamic data
				args = append(args, chartDynamicData(data.Type+":"+data.Name+"="+data.Value, c)...)
				break
			}
		}

		if !dynamicData {
			args = append(args, data.Type+":"+data.Name+"="+data.Value)
		}
	}

	for _, cmd := range tmpl.Script {
		dynamicData = false
		for k := range c {
			if strings.Contains(cmd.Name, k) || strings.Contains(cmd.Value, k) {
				dynamicData = true
				// Process dynamic data
				args = append(args, chartDynamicData(cmd.Name+":"+cmd.Value, c)...)
				break
			}
		}

		if !dynamicData {
			args = append(args, cmd.Name+":"+cmd.Value)
		}
	}

	cmd := exec.Command("rrdtool", args...)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Unable to generate chart. %s. %s", err, string(buf))
	}
	return nil
}

func chartDynamicData(value string, c map[string][]string) []string {
	args := []string{}
	for k, v := range c {
		for i := range v {
			if len(args) != len(v) {
				args = append(args, value)
			}
			// Sanitize dynamic data
			val := v[i]

			// Replace placeholders with values
			args[i] = strings.Replace(args[i], k, val, -1)
		}
	}
	return args
}
