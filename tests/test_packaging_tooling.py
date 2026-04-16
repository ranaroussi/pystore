"""
Tests for packaging metadata and release tooling.
"""

import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pystore

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_COPY_IGNORE = shutil.ignore_patterns(
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "__pycache__",
    "build",
    "dist",
    "*.egg-info",
)


class TestPackagingTooling:
    """Test packaging metadata and release workflow configuration."""

    def test_build_outputs_include_typing_marker_and_metadata(self, tmp_path):
        """Building the project should produce distributions with expected metadata."""
        source_dir = tmp_path / "source"
        dist_dir = tmp_path / "dist"
        shutil.copytree(REPO_ROOT, source_dir, ignore=PROJECT_COPY_IGNORE)

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--outdir",
                str(dist_dir),
                str(source_dir),
            ],
            capture_output=True,
            check=False,
            cwd=source_dir,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr

        wheel_path = next(dist_dir.glob("pystore-*.whl"))
        sdist_path = next(dist_dir.glob("pystore-*.tar.gz"))

        with zipfile.ZipFile(wheel_path) as wheel_archive:
            wheel_names = wheel_archive.namelist()
            metadata_name = next(
                name for name in wheel_names if name.endswith(".dist-info/METADATA")
            )
            metadata = wheel_archive.read(metadata_name).decode()

        assert "pystore/py.typed" in wheel_names
        assert f"Version: {pystore.__version__}" in metadata
        assert "Requires-Python: >=3.9" in metadata
        assert "License-Expression: Apache-2.0" in metadata

        with tarfile.open(sdist_path, "r:gz") as sdist_archive:
            sdist_names = sdist_archive.getnames()

        assert any(name.endswith("pystore/py.typed") for name in sdist_names)
        assert any(name.endswith("pyproject.toml") for name in sdist_names)

    def test_pyproject_uses_single_version_source(self):
        """The project metadata should source the version from the package."""
        pyproject = (REPO_ROOT / "pyproject.toml").read_text()

        assert 'dynamic = ["version"]' in pyproject
        assert 'version = {attr = "pystore.__version__"}' in pyproject
        assert "setuptools-scm" not in pyproject

    def test_publish_workflow_uses_modern_build_and_publish_steps(self):
        """The publish workflow should use the modern PyPA build and publish path."""
        workflow = (REPO_ROOT / ".github" / "workflows" / "python-publish.yml").read_text()

        assert "types: [published]" in workflow
        assert "actions/checkout@v4" in workflow
        assert "actions/setup-python@v5" in workflow
        assert "python -m build" in workflow
        assert "python -m twine check dist/*" in workflow
        assert "actions/upload-artifact@v4" in workflow
        assert "actions/download-artifact@v4" in workflow
        assert "id-token: write" in workflow
        assert "pypa/gh-action-pypi-publish@release/v1" in workflow
        assert "setup.py sdist bdist_wheel" not in workflow
