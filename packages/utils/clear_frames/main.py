import argparse
import os
from pathlib import Path
from typing import Tuple

from fileseq import FrameSet, findSequencesOnDisk

FILE_SIZE_THRESHOLD = 10000


def parse_args() -> Tuple[Path, FrameSet]:
    parser = argparse.ArgumentParser(description="Delete any file under 10 Ko")
    parser.add_argument(
        "--folder", type=str, help="Folder that contains the files to check"
    )
    parser.add_argument("--frange", type=str, help="Frame range of files to check")

    args = parser.parse_args()
    return Path(args.folder), FrameSet(args.frange)


def clear_frames(folder: Path, frange: FrameSet):
    if not folder.exists():
        print(f"INFO: The folder {folder} does not exists")
        return

    sequences = findSequencesOnDisk(folder.as_posix())

    for sequence in [s for s in sequences if len(s) > 1]:
        for index in [f for f in frange if isinstance(f, int)]:
            file = Path(sequence.frame(index))
            if not file.exists():
                # print(f"INFO: The file {file} does not exists")
                continue

            try:
                size = os.path.getsize(file)
                if size < FILE_SIZE_THRESHOLD:
                    os.remove(file)
                    print(
                        f"[CLEAN EMPTY FRAMES]: The file {file} of {size/1000}kb has been deleted"
                    )
                else:
                    # print(f"INFO: Skipping the file {file} of {size/1000}kb")
                    pass
            except Exception:
                pass


def main():
    clear_frames(*parse_args())


if __name__ == "__main__":
    main()
