"""
Tests for packaging metadata and release tooling.
"""

import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_INIT = REPO_ROOT / "pystore" / "__init__.py"
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


def _load_package_version() -> str:
    """Load the package version without importing the runtime package graph."""
    match = re.search(
        r'^__version__\s*=\s*"([^"]+)"',
        PACKAGE_INIT.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    assert match is not None, "Could not determine pystore.__version__"
    return match.group(1)


PACKAGE_VERSION = _load_package_version()


class TestPackagingTooling:
    """Test packaging metadata and release workflow configuration."""

    def test_dependency_manifests_pin_exact_versions(self):
        """Dependency manifests should use exact pins for direct dependencies."""
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")

        expected_pyproject_entries = [
            'requires = ["setuptools==77.0.3"]',
            '"pandas==2.3.1",',
            '"pyarrow==21.0.0",',
            "\"dask[complete]==2024.8.0; python_version < '3.10'\",",
            "\"dask[complete]==2025.7.0; python_version >= '3.10'\",",
            '"numpy==1.26.4",',
            '"fsspec==2025.7.0",',
            '"toolz==1.0.0",',
            '"cloudpickle==3.1.1",',
            '"python-snappy==0.7.3",',
            '"partd==1.4.2",',
            '"psutil==7.0.0",',
            '"pytest==8.4.1",',
            '"pytest-asyncio==1.1.0",',
            '"pytest-cov==6.2.1",',
            '"pytest-xdist==3.8.0",',
            '"black==25.1.0",',
            '"ruff==0.12.4",',
            '"mypy==1.17.0",',
            "\"pandas-stubs==2.2.2.240807; python_version < '3.10'\",",
            "\"pandas-stubs==2.3.0.250703; python_version >= '3.10'\",",
            '"types-setuptools==80.9.0.20250529",',
            '"build==1.4.2",',
            "\"sphinx==7.4.7; python_version < '3.10'\",",
            "\"sphinx==8.1.3; python_version == '3.10'\",",
            "\"sphinx==8.2.3; python_version >= '3.11'\",",
            '"sphinx-rtd-theme==3.0.2",',
            "\"sphinx-autodoc-typehints==2.3.0; python_version < '3.10'\",",
            "\"sphinx-autodoc-typehints==3.0.1; python_version == '3.10'\",",
            "\"sphinx-autodoc-typehints==3.2.0; python_version >= '3.11'\",",
        ]

        expected_requirements = [
            "pandas==2.3.1",
            "pyarrow==21.0.0",
            'dask[complete]==2024.8.0; python_version < "3.10"',
            'dask[complete]==2025.7.0; python_version >= "3.10"',
            "numpy==1.26.4",
            "fsspec==2025.7.0",
            "toolz==1.0.0",
            "cloudpickle==3.1.1",
            "python-snappy==0.7.3",
            "partd==1.4.2",
        ]

        for entry in expected_pyproject_entries:
            assert entry in pyproject

        for entry in expected_requirements:
            assert entry in requirements

        old_specifiers = [
            "setuptools>=",
            "pandas>=",
            "pyarrow>=",
            "dask[complete]>=",
            "numpy>=",
            "fsspec>=",
            "toolz>=",
            "cloudpickle>=",
            "python-snappy>=",
            "partd>=",
            "psutil>=",
            "pytest>=",
            "pytest-asyncio>=",
            "pytest-cov>=",
            "pytest-xdist>=",
            "black>=",
            "build>=",
            "ruff>=",
            "mypy>=",
            "pandas-stubs>=",
            "sphinx>=",
            "sphinx-rtd-theme>=",
            "sphinx-autodoc-typehints>=",
        ]

        for specifier in old_specifiers:
            assert specifier not in pyproject
            assert specifier not in requirements

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

        assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"

        wheel_path = next(dist_dir.glob("pystore-*.whl"))
        sdist_path = next(dist_dir.glob("pystore-*.tar.gz"))

        with zipfile.ZipFile(wheel_path) as wheel_archive:
            wheel_names = wheel_archive.namelist()
            metadata_name = next(
                name for name in wheel_names if name.endswith(".dist-info/METADATA")
            )
            metadata = wheel_archive.read(metadata_name).decode()

        assert "pystore/py.typed" in wheel_names
        assert f"Version: {PACKAGE_VERSION}" in metadata
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

    def test_conda_recipe_tracks_current_release_metadata(self):
        """The conda recipe should match the current package release metadata."""
        recipe = (REPO_ROOT / "meta.yaml").read_text(encoding="utf-8")

        assert '{% set name = "pystore" %}' in recipe
        assert f'{{% set version = "{PACKAGE_VERSION}" %}}' in recipe
        assert (
            "https://pypi.io/packages/source/{{ name[0] }}/{{ name }}/"
            "{{ name }}-{{ version }}.tar.gz"
        ) in recipe
        assert 'license_file: "LICENSE.txt"' in recipe
        assert "- fsspec ==2025.7.0" in recipe

        # Conda recipe dependencies should use exact version pins
        expected_conda_pins = [
            "- cloudpickle ==3.1.1",
            "- dask ==2024.8.0",
            "- distributed ==2024.8.0",
            "- fsspec ==2025.7.0",
            "- numpy ==1.26.4",
            "- pandas ==2.3.1",
            "- partd ==1.4.2",
            "- pyarrow ==21.0.0",
            "- python-snappy ==0.7.3",
            "- toolz ==1.0.0",
        ]
        for pin in expected_conda_pins:
            assert pin in recipe, f"Expected conda pin {pin!r} not found in meta.yaml"

        # Old loose specifiers should not be present
        old_conda_specifiers = [
            "cloudpickle >=",
            "dask >=",
            "distributed >=",
            "fsspec >=",
            "numpy >=",
            "pandas >=",
            "partd >=",
            "pyarrow >=",
            "python-snappy >=",
            "toolz >=",
        ]
        for specifier in old_conda_specifiers:
            assert specifier not in recipe, f"Old specifier {specifier!r} still present in meta.yaml"

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

        # Build tools should use pinned versions, not --upgrade
        assert "pip install pip==25.2 build==1.4.2 twine==6.1.0" in workflow
        assert "--upgrade" not in workflow
