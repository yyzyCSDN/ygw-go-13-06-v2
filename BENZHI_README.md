# Docker verification

Build with `sh build_benzhi_docker.sh`. The image retains the complete Go toolchain and downloads module dependencies at build time. Start an editable workspace with `docker run --rm -it -v "$PWD:/workspace" segment-merger-benzhi`.
