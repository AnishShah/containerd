/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package command

import (
	"os"
	"path/filepath"
	"testing"

	srvconfig "github.com/containerd/containerd/v2/cmd/containerd/server/config"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	primaryRoot := filepath.Join(t.TempDir(), "primary")
	stateDir := filepath.Join(t.TempDir(), "state")
	sec1 := t.TempDir()
	sec2 := t.TempDir()
	nonExistentSec := filepath.Join(t.TempDir(), "does-not-exist")
	fileSec := filepath.Join(t.TempDir(), "file-not-dir")
	require.NoError(t, os.WriteFile(fileSec, []byte("test"), 0o600))

	t.Run("valid without secondary_roots", func(t *testing.T) {
		err := validateConfig(&srvconfig.Config{
			Root:  primaryRoot,
			State: stateDir,
		})
		require.NoError(t, err)
	})

	t.Run("valid with secondary_roots", func(t *testing.T) {
		err := validateConfig(&srvconfig.Config{
			Root:           primaryRoot,
			State:          stateDir,
			SecondaryRoots: []string{sec1, sec2},
		})
		require.NoError(t, err)
	})

	cases := []struct {
		name    string
		config  *srvconfig.Config
		wantErr string
	}{
		{
			name: "empty root",
			config: &srvconfig.Config{
				Root:  "",
				State: stateDir,
			},
			wantErr: "root must be specified",
		},
		{
			name: "empty state",
			config: &srvconfig.Config{
				Root:  primaryRoot,
				State: "",
			},
			wantErr: "state must be specified",
		},
		{
			name: "same path for root and state",
			config: &srvconfig.Config{
				Root:  primaryRoot,
				State: primaryRoot,
			},
			wantErr: "root and state must be different paths",
		},
		{
			name: "empty secondary_roots entry",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1, ""},
			},
			wantErr: "secondary_roots entry must not be empty",
		},
		{
			name: "secondary_roots matches primary root",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{primaryRoot},
			},
			wantErr: "must be different from root",
		},
		{
			name: "secondary_roots matches primary root with trailing slash",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{primaryRoot + string(filepath.Separator)},
			},
			wantErr: "must be different from root",
		},
		{
			name: "secondary_roots matches state dir",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{stateDir},
			},
			wantErr: "must be different from state",
		},
		{
			name: "duplicate secondary_roots entries",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1, sec1},
			},
			wantErr: "duplicate secondary_roots entry",
		},
		{
			name: "duplicate secondary_roots entries after clean",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1, sec1 + string(filepath.Separator) + "."},
			},
			wantErr: "duplicate secondary_roots entry",
		},
		{
			name: "non-existent secondary_roots entry",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1, nonExistentSec},
			},
			wantErr: "invalid secondary_roots entry",
		},
		{
			name: "secondary_roots entry is not a directory",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1, fileSec},
			},
			wantErr: "is not a directory",
		},
		{
			name: "secondary_roots entry contains comma",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1 + ",extra"},
			},
			wantErr: "must not contain",
		},
		{
			name: "secondary_roots entry contains path list separator",
			config: &srvconfig.Config{
				Root:           primaryRoot,
				State:          stateDir,
				SecondaryRoots: []string{sec1 + string(filepath.ListSeparator) + "extra"},
			},
			wantErr: "must not contain",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.config)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	t.Run("duplicate secondary_roots loaded via LoadConfigWithPlugins", func(t *testing.T) {
		tomlPath := filepath.Join(t.TempDir(), "config.toml")
		tomlContent := "version = 3\nroot = " + `"` + filepath.ToSlash(primaryRoot) + `"` + "\nstate = " + `"` + filepath.ToSlash(stateDir) + `"` + "\nsecondary_roots = [" + `"` + filepath.ToSlash(sec1) + `", "` + filepath.ToSlash(sec1) + `"` + "]\n"
		require.NoError(t, os.WriteFile(tomlPath, []byte(tomlContent), 0o600))

		cfg := defaultConfig()
		require.NoError(t, srvconfig.LoadConfigWithPlugins(t.Context(), tomlPath, nil, cfg))
		require.ErrorContains(t, validateConfig(cfg), "duplicate secondary_roots entry")
	})
}
