from __future__ import print_function

import argparse
import os
import sys

#!/usr/bin/env hython


def error(msg, exit=True):
    if msg:
        sys.stderr.write("\n")
        sys.stderr.write("Error: %s\n" % msg)
        sys.stderr.write("*****")

    sys.stderr.write("\n")
    if exit:
        sys.exit(1)


def usage(msg=""):
    print(
        """
Usage:

Single frame:   hrender    [options] driver|cop file.hip [imagefile]
Frame range:    hrender -e [options] driver|cop file.hip

driver|cop:     -c /img/imgnet
                -c /img/imgnet/cop_name
                -d output_driver

options:        -w pixels       Output width
                -h pixels       Output height
                -F frame        Single frame
                -b fraction     Image processing fraction (0.01 to 1.0)
                -t take		Render a specified take
                -o output       Output name specification
                -v              Run in verbose mode
                -I              Interleaved, hscript render -I

with \"-e\":	-f start end    Frame range start and end
                -i increment    Frame increment

Notes:  1)  For output name use $F to specify frame number (e.g. -o $F.pic).
        2)  If only one of width (-w) or height (-h) is specified, aspect ratio
            will be maintained based upon aspect ratio of output driver."""
    )
    error(msg)


def validate_args(args):
    # Does some light validation on the input and exits on error.

    hipfiles = []
    for f in args.file:
        if "." in f and f.split(".")[-1] in ("hip", "hipnc", "hiplc"):
            hipfiles.append(f)

    if not hipfiles:
        return "Missing .hip motion file name."
    if len(hipfiles) > 1:
        return "Too many .hip motion file names: %s." % (" ".join(hipfiles))
    if not os.path.isfile(hipfiles[0]):
        return "Cannot find file %s." % hipfiles[0]

    args.file = hipfiles[0]

    if args.frame_range:
        if not args.e_option:
            return "Cannot specify frame range without -e."
        if args.frame_range[0] > args.frame_range[1]:
            return "Start frame cannot be greater than end frame."

    if args.i_option:
        if not args.e_option:
            return "Cannot use -i option without -e."
    if not args.c_option and not args.d_option:
        return "Must specify one of -c or -d."
    if args.c_option and args.d_option:
        return "Cannot specify both -c and -d."
    if args.w_option and args.w_option < 1:
        return "Width must be greater than zero."
    if args.h_option and args.h_option < 1:
        return "Height must be greater than zero."
    if args.i_option and args.i_option < 1:
        return "Frame increment must be greater than zero."

    if args.c_option:
        if args.c_option[-1] == "/":
            return "Invalid parameter for -c option (trailing '/')"
    return ""


def parse_args():
    """Parses the command line arguments, then validates them. If there are any
    validation errors, those are sent to usage() which terminates the process.
    """
    parser = argparse.ArgumentParser(add_help=False)

    # Option arguments
    parser.add_argument("-c", dest="c_option")
    parser.add_argument("-d", dest="d_option")
    parser.add_argument("-w", dest="w_option", type=int)
    parser.add_argument("-h", dest="h_option", type=int)
    parser.add_argument("-i", dest="i_option", type=int)
    parser.add_argument("-t", dest="t_option")
    parser.add_argument("-o", dest="o_option")
    parser.add_argument("-b", dest="b_option", type=float)
    parser.add_argument("-j", dest="threads", type=int)
    parser.add_argument("-F", dest="frame", type=float)
    parser.add_argument("-f", dest="frame_range", nargs=2, type=float)

    # .hip|.hiplc|.hipnc file
    parser.add_argument("file", nargs="*")

    # Boolean flags
    parser.add_argument("-e", dest="e_option", action="store_true")
    parser.add_argument("-R", dest="renderonly", action="store_true")
    parser.add_argument("-v", dest="v_option", action="store_true")
    parser.add_argument("-I", dest="I_option", action="store_true")

    args, unknown = parser.parse_known_args()

    # Handle unknown arguments (show usage text and exit)
    if unknown:
        usage("Unknown argument(s): %s" % (" ".join(unknown)))

    # If there's something wrong with the arguments, show usage and exit.
    err = validate_args(args)
    if err:
        usage(err)

    return args


def get_output_node(args):
    # Returns the output node to use (driver|cop).

    if args.c_option:
        comp = hou.node("/out").createNode("comp")
        comp.parm("coppath").set(args.c_option)
        rop = "/out/%s" % comp.name()
    else:
        # If a leading slash was provided, it's an absolute path.
        if args.d_option[0] == "/":
            rop = args.d_option
        else:
            rop = "/out/%s" % args.d_option

    return hou.node(rop)


def set_aspect_ratio(args, rop_node):
    # Sets the appropriate width and height based on the current aspect ratio
    # if a width or height is provided.

    # Maintain aspect ratio if the width or height is given, but not both.
    keep_aspect = bool(args.w_option) ^ bool(args.h_option)

    if keep_aspect:
        if args.d_option:
            xres = rop_node.parm("res_overridex").eval()
            yres = rop_node.parm("res_overridey").eval()
        else:
            xres = rop_node.parm("res1").eval()
            yres = rop_node.parm("res2").eval()

        if args.w_option:
            args.h_option = int((float(args.w_option) / xres) * yres)
        else:
            args.w_option = int((float(args.h_option) / yres) * xres)


def set_overrides(args, rop_node):
    # If a width or height is specified, we should override the resolution.
    if args.w_option or args.h_option:
        if args.d_option:
            rop_node.parm("override_camerares").set(True)
            rop_node.parm("res_fraction").set("specific")
            rop_node.parm("res_overridex").set(args.w_option)
            rop_node.parm("res_overridey").set(args.h_option)
        else:
            rop_node.parm("tres").set("specify")
            rop_node.parm("res1").set(args.w_option)
            rop_node.parm("res2").set(args.h_option)

    # Override the output file name.
    if args.o_option:
        if args.c_option:
            rop_node.parm("copoutput").set(args.o_option)
        else:
            output_file_parm = "vm_picture"

            # If it's a V-Ray render
            if rop_node.type().name() == "vray_renderer":
                output_file_parm = "SettingsOutput_img_file_path"

            rop_node.parm(output_file_parm).set(args.o_option.replace("\\", "/"))

    # Add image processing fraction.
    if args.b_option:
        rop_node.parm("fraction").set(args.b_option)

    if args.t_option:
        rop_node.parm("take").set(args.t_option)


def set_frame_range(args, rop_node):
    # Sets frame range information on the output node.
    increment = args.i_option or 1

    frame_range = ()
    if args.frame_range:
        # Render the given frame range.
        rop_node.parm("trange").set(1)
        frame_range = (args.frame_range[0], args.frame_range[1], increment)
    elif args.frame:
        # Render single frame (start and end frames are the same).
        rop_node.parm("trange").set(1)
        frame_range = (args.frame, args.frame, increment)
    else:
        # Render current frame.
        rop_node.parm("trange").set(0)

    return frame_range


def render(args):
    try:
        hou.hipFile.load(args.file)
    except hou.LoadWarning as e:
        print(e)

    rop_node = get_output_node(args)

    set_aspect_ratio(args, rop_node)
    set_overrides(args, rop_node)
    frame_range = set_frame_range(args, rop_node)

    interleave = (
        hou.renderMethod.FrameByFrame if args.I_option else hou.renderMethod.RopByRop
    )

    rop_node.render(
        verbose=bool(args.v_option), frame_range=frame_range, method=interleave
    )


# --------------------------------------------------------
# Main application
# --------------------------------------------------------

args = sys.argv[1:]
if len(args) < 1 or args[0] == "-":
    usage()

args = parse_args()
render(args)

