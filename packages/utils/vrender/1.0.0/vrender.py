import argparse
import os
import pathlib
from datetime import datetime

# Global object holding progress information
CONTEXT = {"progress": 0.0, "total_frames": 0}

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


def log_info(msg):
    log("info", msg)


def print_alfred_progress(p):
    print("TR_PROGRESS {}%".format(str(p)).zfill(3))


def printProgress(renderer, message, progress, total, instant):
    progress = round(100.0 * progress / total) if total != 0 else 0

    if progress - (progress // 5) * 5 <= 1:
        log("progress", "{0} {1}%".format(message, progress))

        if message == "Rendering image...":
            sequence_progress = CONTEXT["progress"] + (
                progress / CONTEXT["total_frames"]
            )
            print_alfred_progress(sequence_progress)


def separator():
    print("-" * 50)


def log_with_header(msg):
    separator()
    print(msg)
    separator()


def dumpMsg(renderer, message, level, instant):
    """Register a simple log callback. Always useful for debugging"""
    import vray

    if level == vray.LOGLEVEL_ERROR:
        log("error", message)
    elif level == vray.LOGLEVEL_WARNING:
        log("warning", message)
    elif level == vray.LOGLEVEL_INFO:
        log_info(message)


def frame_already_exist(args, i):
    """Test if the frame i already exists"""
    out_filename = (
        f"{args.imgFile.with_suffix('')}.{str(i).zfill(4)}{args.imgFile.suffix}"
    )
    existing = os.path.exists(out_filename)

    if existing and log:
        log_info(f"FRAME {i} ALREADY EXISTS ({out_filename})")

    return existing


def get_frames(args):
    """Returns the list of frames to render"""
    return list(set(map(int, args.frames.split(";"))))


def check_all_frames_existing(args):
    """Check if all the frames need to be skipped"""
    if all([frame_already_exist(args, f) for f in get_frames(args)]):
        import sys

        log_info("ALL THE FRAMES WERE SKIPPED")
        sys.exit(0)


def render(args):

    if args.skipExistingFrames == 1:
        check_all_frames_existing(args)

    log_info("LOADING VRAY SDK")
    import vray

    log_info("VRAY SDK LOADED")

    renderer_args = {
        "enableFrameBuffer": False,
        "showFrameBuffer": False,
    }

    rtEngineMapping = {
        "regular": "production",
        "cuda": "productionCuda",
        "optix": "productionOptix",
    }

    # Create an instance of VRayRenderer with default options.
    # The renderer is automatically closed after the `with` block.
    with vray.VRayRenderer(**renderer_args) as renderer:
        rtEngine = rtEngineMapping[args.rtEngine]
        log_info(f"Using {rtEngine} ray tracing engine")
        renderer.renderMode = rtEngine

        renderer.setOnLogMessage(dumpMsg)

        # Add a listener for progress event.
        # It is invoked every time a chunk of work is finished and
        # provides information about what part of the whole work is done.
        renderer.setOnProgress(printProgress)

        # LOAD FILE
        renderer.load(args.sceneFile.as_posix())

        # OUTPUT SETTINGS
        settingsOutput = renderer.classes.SettingsOutput.getInstanceOrCreate()

        # Image directory output
        img_dir = str(args.imgFile.parents[0].as_posix()).rstrip("/")
        settingsOutput.img_dir = f"{img_dir}/"

        # Image file output
        settingsOutput.img_file = f"{args.imgFile.stem}.{args.imgFile.suffix}"
        settingsOutput.img_file_needFrameNumber = True

        # Override resolution if needed
        if args.imgWidth or args.imgHeight:
            renderer.size = (args.imgWidth, args.imgHeight)

        # Filter frames to render
        existing_frames = [
            f
            for f in get_frames(args)
            if args.skipExistingFrames == 0 or not frame_already_exist(args, f)
        ]

        CONTEXT["total_frames"] = len(existing_frames)

        renderer.renderSequence(existing_frames)
        total_frames_time = 0

        print_alfred_progress(0)

        for i, frame in enumerate(existing_frames):
            CONTEXT["progress"] = (
                (i / len(existing_frames)) * 100 if len(existing_frames) > 0 else 0
            )

            before_frame = datetime.now()
            log_with_header(f"RENDERING FRAME {frame}")

            # Wait for frame to finish rendering.
            renderer.waitForRenderEnd()

            # Compute frame time
            after_frame = datetime.now()
            frame_duration = (after_frame - before_frame).total_seconds()
            total_frames_time += frame_duration

            separator()
            log_info(
                f"FRAME {frame} RENDERED IN {human_time_duration(frame_duration)} (load + build + render)"
            )
            separator()

            # Continue rendering next frame.
            renderer.continueSequence()

        print_alfred_progress(100)

        average_frame_render_time = total_frames_time / len(existing_frames)

        separator()
        log_info(
            f"AVERAGE RENDER TIME PER FRAME: {human_time_duration(average_frame_render_time)} ({len(existing_frames)} frames)"
        )
        separator()

        log_info("END RENDERING")
        log_info("CLOSING VRAY")

        vray.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-imgFile", help="Output image file path", type=pathlib.Path, required=True
    )
    parser.add_argument("-imgWidth", help="Image width in pixels", type=int)
    parser.add_argument("-imgHeight", help="Image height in pixels", type=int)
    parser.add_argument("-sceneFile", help="Vrscene path", type=pathlib.Path)
    parser.add_argument(
        "-skipExistingFrames", help="Skip already existing frames", type=int, default=0
    )
    parser.add_argument("-frames", help="List of frames", type=str, required=True)
    parser.add_argument(
        "-rtEngine",
        help="Ray tracing engine (regular, cuda, optix)",
        type=str,
        default="regular",
    )

    args = parser.parse_args()

    render(args)


if __name__ == "__main__":
    main()
