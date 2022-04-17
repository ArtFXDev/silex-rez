import argparse
import io
import os
import pathlib
import re
import subprocess
import sys
from datetime import datetime

import fileseq

TIME_DURATION_UNITS = (
    ("week", 60 * 60 * 24 * 7),
    ("day", 60 * 60 * 24),
    ("hour", 60 * 60),
    ("min", 60),
    ("sec", 1),
)

# Taken from https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
def human_time_duration(seconds):
    if seconds == 0:
        return "inf"
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append("{} {}{}".format(amount, unit, "" if amount == 1 else "s"))
    return ", ".join(parts)


def log(level, msg):
    print(f"[{datetime.now().strftime('%m/%d/%Y, %H:%M:%S')}] [{level.upper()}]", msg)


def separator():
    print("-" * 50)


def log_info(msg):
    log("info", msg)


def log_info_sep(msg):
    separator()
    log_info(msg)
    separator()


def print_alfred_progress(p):
    print("TR_PROGRESS {}%".format(str(p)).zfill(3))


def log_with_header(msg):
    separator()
    print(msg)
    separator()


def frame_already_exist(args, i):
    """Test if the frame i already exists"""
    out_filename = (
        f"{args.imgFile.with_suffix('')}.{str(i).zfill(4)}{args.imgFile.suffix}"
    )
    existing = os.path.exists(out_filename)

    if existing:
        log_info(f"FRAME {i} ALREADY EXISTS ({out_filename})")

    return existing


def get_frames(args):
    """Returns the list of frames to render"""
    frames = list(set(args.frames))
    frames.sort()
    return frames


def check_all_frames_existing(args):
    """Check if all the frames need to be skipped"""
    if all([frame_already_exist(args, f) for f in get_frames(args)]):
        import sys

        log_info("ALL THE FRAMES WERE SKIPPED")
        sys.exit(0)


def render(args):
    if args.skipExistingFrames == 1:
        check_all_frames_existing(args)

    ass_files = [
        pathlib.Path(f)
        for f in os.listdir(args.assFolder)
        if pathlib.Path(f).suffix == ".ass"
    ]

    if len(ass_files) == 0:
        log("error", f"NO ASS FILES WERE FOUND IN FOLDER {args.assFolder}")
        sys.exit(-1)

    found_sequences = fileseq.findSequencesOnDisk(args.assFolder / "*.ass")

    if len(found_sequences) == 0:
        log("error", f"NO ASS SEQUENCES WERE FOUND IN FOLDER {args.assFolder}")
        sys.exit(-1)

    ass_sequence = found_sequences[0]
    ass_frames = list(ass_sequence.frameSet())
    frames = get_frames(args)

    # Check for missing ass files
    missing_frames = set(frames) - set(ass_frames)
    if len(missing_frames) > 0:
        log("warning", f"MISSING ASS FILES FOR FRAMES: {missing_frames}")

    # The frames that will be rendered are the existing ass files in the sequence
    # Also skip existing frames
    frames_to_render = [
        f
        for f in set(frames).intersection(ass_frames)
        if args.skipExistingFrames == 0 or not frame_already_exist(args, f)
    ]

    if len(frames_to_render) == 0:
        log_info(f"NO FRAMES TO RENDER")
        sys.exit(0)
    else:
        log_info(f"FRAMES TO RENDER: {frames_to_render}")

    # Creating output folder
    output_folder = args.imgFile.parents[0]

    if not os.path.exists(output_folder):
        log_info(f"CREATING OUTPUT FOLDER: {output_folder.as_posix()}")
        os.makedirs(output_folder)

    print_alfred_progress(0)
    total_frames_time = 0

    for i, frame in enumerate(frames_to_render):
        log_with_header(f"RENDERING FRAME {frame}")
        before_frame = datetime.now()

        output_file = (
            f"{args.imgFile.with_suffix('')}.{str(frame).zfill(4)}{args.imgFile.suffix}"
        )

        kick_command = [
            "kick",
            "-dw",
            "-dp",
            "-v 3",
            "-nocrashpopup",
            "-nostdin",
            f"-set driver_exr.filename {output_file}",
            f"-o {output_file}",
            f"-i {ass_sequence.frame(frame)}",
        ]

        if args.pluginLibraries:
            plugin_libraries = [l for ll in args.pluginLibraries for l in ll]
            kick_command += [f"-l {lib}" for lib in plugin_libraries]

        progress = (i / len(frames_to_render)) * 100
        print_alfred_progress(progress)

        kick_subprocess = subprocess.Popen(
            " ".join(kick_command), stdout=subprocess.PIPE, cwd="C:/Users"
        )

        if not kick_subprocess.stdout:
            log("error", "COULD NOT GET STDOUT")
            sys.exit(-1)

        for line in io.TextIOWrapper(kick_subprocess.stdout, encoding="utf-8"):
            if "rays/pixel" in line:
                match = re.search(r".+\D(\d+)% done.+", line)
                if match:
                    frame_progress = int(match.group(1))
                    print_alfred_progress(
                        progress + (frame_progress / len(frames_to_render))
                    )

            print(line, end="")

        kick_subprocess.wait()
        log_info(f"RETURN CODE: {kick_subprocess.returncode}")

        if kick_subprocess.returncode != 0:
            log("error", "KICK ERROR, STOPPING RENDER")
            sys.exit(-1)

        # Compute frame time
        after_frame = datetime.now()
        frame_duration = (after_frame - before_frame).total_seconds()
        total_frames_time += frame_duration

        log_info_sep(
            f"FRAME {frame} RENDERED IN {human_time_duration(frame_duration)} (load + build + render)"
        )

    average_frame_render_time = total_frames_time / len(frames_to_render)
    log_info_sep(
        f"AVERAGE RENDER TIME PER FRAME: {human_time_duration(average_frame_render_time)} ({len(frames_to_render)} frames)"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-assFolder",
        help="Folder of .ass files to render",
        type=pathlib.Path,
        required=True,
    )
    parser.add_argument(
        "-frames",
        help="List of frames to render, fileseq FrameSet",
        type=fileseq.FrameSet,
        required=True,
    )
    parser.add_argument(
        "-skipExistingFrames", help="Skip already existing frames", type=int, default=0
    )
    parser.add_argument(
        "-imgFile", help="Output image file path", type=pathlib.Path, required=True
    )
    parser.add_argument(
        "-pluginLibraries",
        action="append",
        help="Folder for dlls plugins to be loaded by Arnold",
        nargs="*",
    )

    args = parser.parse_args()

    if not os.path.exists(args.assFolder):

        log("error", f"ASS FOLDER {args.assFolder} DOESNT EXIST")
        sys.exit(-1)

    render(args)


if __name__ == "__main__":
    main()
