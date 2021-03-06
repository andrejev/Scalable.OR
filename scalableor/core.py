# -*- coding: utf-8 -*-

# system libs
import argparse
import atexit
import json
import sys
import os
import zipfile

# external libs
from pyspark import SparkContext, SparkConf

# local imports
import log
import method
import verify

from constant import NAME
from manager import VerifiersManager, MethodsManager

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))


class ScalableOR(object):
    sc = None
    zippath = os.path.abspath(os.path.join(CURRENT_DIR, "../scalable.zip"))

    def get_jars(self, from_path="vendor/jar"):
        """
        Return required java paths for SparkContext
        """
        path_jar = os.path.join(PROJECT_DIR, from_path)
        return [os.path.join(path_jar, i) for i in os.listdir(path_jar)]

    def get_pythons(self):
        """
        Return required python paths for SparkContext
        """
        included_libraries = [ScalableOR.zippath]
        if self.args.include_python_libraries:
            included_libraries += self.args.include_python_libraries.split(",")
        return included_libraries

    def initialize_spark(self, args):
        """
        initialize spark context

        :param args:    parsed command line arguments
        """
        if args.in_proc:
            master = "local"
        else:
            master = args.master

        if "SPARK_HOME" not in os.environ:
            os.environ["SPARK_HOME"] = args.spark_home

        os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars %s pyspark-shell" % ",".join(self.get_jars())
        ScalableOR.sc = SparkContext(master=master, appName=NAME)
        map(ScalableOR.sc.addPyFile, self.get_pythons())
        log.set_logger(ScalableOR.sc, args.verbose)

    def __init__(self, argv=None):
        atexit.register(self.on_exit)
        self.args = args = self.get_args(argv)

        # create zip file of current project (required for distributed execution)
        if not os.path.exists(ScalableOR.zippath):
            self.zipdir(os.path.join(PROJECT_DIR, "scalableor"))

        # create spark context instance
        if ScalableOR.sc is None:
            self.initialize_spark(args)

        # validate required parameters
        if None in [args.input, args.output, args.or_program]:
            raise ValueError("please define all required parameters.")

        # validate path parameters
        i_path = os.path.abspath(args.input)
        o_path = os.path.abspath(args.output)
        op_path = os.path.abspath(args.or_program)

        for p in [i_path, op_path, os.path.dirname(o_path)]:
            if not os.path.exists(p):
                raise ValueError("path '%s' doesn't exist." % p)

        if os.path.exists(o_path):
            log.logger.info("output file already exists. Remove it (%s)" % i_path)
            os.remove(o_path)

        log.logger.debug("parser args: %s" % args)
        log.logger.info("input path: %s" % i_path)
        log.logger.info("output path: %s" % o_path)
        log.logger.info("or-program path: %s" % op_path)

        # read or-program
        or_program = json.load(open(op_path, "r"))

        # import file
        if args.add_import_command:
            or_program.insert(0, {"op": "scalableor/import", "separator": ",", "path": i_path})

        # export file
        if args.add_export_command:
            or_program.append({"op": "scalableor/export", "separator": ",", "path": o_path})

        # verify or-program
        if self.verify(or_program) is False:
            log.logger.error("verifying is failed")
            sys.exit(1)

        self.refine(or_program)

    def on_exit(self):
        """
        exit handler
        """
        if os.path.exists(ScalableOR.zippath):
            log.logger.info("clear: remove '%s'" % ScalableOR.zippath)
            os.remove(ScalableOR.zippath)

    def zipdir(self, path):
        """
        create zip archive

        :param path: source directory
        """
        zipf = zipfile.ZipFile(ScalableOR.zippath, "w", zipfile.ZIP_DEFLATED)

        basepath_len = len(os.path.dirname(path))
        for root, dirs, files in os.walk(path):
            for f in files:
                if f.endswith(".py"):
                    fpath = os.path.join(root, f)
                    zipf.write(fpath, fpath[basepath_len:])

        zipf.close()

    def get_args(self, argv=None):
        """
        parse command line parameters

        :param argv:            list of command line parameter. By default used os.argv
        :return: parsed parameter
        """
        parser = argparse.ArgumentParser(description="Start %s " % NAME)

        parser.add_argument("-m", "--master", default="spark://localhost:7077",
                            help="set spark master (default: %(default)s)", type=str)

        parser.add_argument("-i", "--input", default=None,
                            help="set path to input file (default: %(default)s)", type=str)

        parser.add_argument("-p", "--or-program", default=None,
                            help="set path to json file with OR program (default: %(default)s)", type=str)

        parser.add_argument("-o", "--output", default=None,
                            help="set path to output file (default: %(default)s)", type=str)

        parser.add_argument("-l", "--in-proc", dest="in_proc", default=False, action="store_true",
                            help="use in-proc spark instance (default: %(default)s)")

        parser.add_argument("--spark-home", default="/usr/local/spark", type=str,
                            help="set path to spark (default: %(default)s)")

        parser.add_argument("-v", "--verbose", action="store_true", default=False,
                            help="increase output verbosity (default: %(default)s)", )

        parser.add_argument("--add-import-command", action="store_true", default=True,
                            help="add command to import input as CSV file (default: %(default)s) ", )

        parser.add_argument("--add-export-command", action="store_true", default=True,
                            help="add command to export result as CSV file (default: %(default)s) ", )

        parser.add_argument("--include-python-libraries", type=str, default=None,
                            help="include python libraries to Spark Context "
                                 "(comma separated list; supported .py,.zip,.egg)", )

        args = parser.parse_args(args=argv)

        return args

    @staticmethod
    def verify(or_program):
        """
        verify OpenRefine program

        :param or_program:      sequence of OpenRefine commands
        :return: validation     status
        """
        error_all = []
        for cmd in or_program:
            name = cmd["op"]
            errors_in = []

            if VerifiersManager.has(name):
                VerifiersManager.call(name, cmd, errors_in)
                error_all += errors_in
            else:
                log.logger.warn("Verify method '%s' doesn't exists" % name)

            if not MethodsManager.has(name):
                error_all.append("Method '%s' doesn't exists" % name)

        if not error_all:
            if not len(or_program):
                error_all.append("OpenRefine command sequence is empty")

        if error_all:
            log.logger.error("Veify error report:")
            log.logger.error("\n".join(error_all))
            return False

        return True

    @staticmethod
    def refine(or_program):
        """
        execute OpenRefine program

        :param or_program:      sequence of OpenRefine commands
        """
        df = None
        samples = []
        for cmd in or_program:
            name = cmd["op"]
            log.logger.info("Call '%s': cmd='%s'" % (name, cmd))
            df = MethodsManager.call(cmd, df=df, sc=ScalableOR.sc)
            df and samples.append(df.head(10))


main = run = ScalableOR
