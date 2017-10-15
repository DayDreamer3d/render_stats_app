""" Module for CLI utility to get the inouts from the user.
"""

import argparse
import os

import config


class Command(object):
    doc = """
        Introduction:
            CLI to parse render logs to reason about them.

        Usage:
            > run.sh --app maya render/logs/dir
            will fetch all the successful renders for maya app.

            > run.sh --renderer rederman --app maya --avg_ram
            get the average ram used for all the renderman renders launched from maya app.
    """
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description=Command.doc,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        self.logs_dir = None
        self.filter_args = None
        self.aggregator_args = None
        self.namespace = None

        self.add_arguments()

    def _add_dir_arg(self):
        """ Add the logs dir argument to the parser.
        """
        dir_ = config.Arguments.args['dir']
        self.parser.add_argument(dir_[0], help=dir_[1], nargs=dir_[2], default=dir_[3]())

    def _add_filter_args(self):
        """ Add filter arguments to the parser which are:
                app, renderer and failed.
        """
        category = 'filters'
        description = config.Arguments.args[category]['description']
        fitler_group = self.parser.add_argument_group(
            category.title(), description
        )

        for arg in config.Arguments.args[category]['arguments']:
            # TODO: bad way of categorising arguments
            # make it better in future

            if len(arg) == 3:
                fitler_group.add_argument('--' + arg[0], '-' + arg[1], help=arg[2], metavar='')
            elif len(arg) == 4:
                fitler_group.add_argument('--' + arg[0], '-' + arg[1], help=arg[2], action=arg[3])

    def _add_aggregator_args(self):
        """ Add the aggregator arguments to the  parser.
            avgtime, avgcpu, avgram, maxram, maxcpu and summary
        """
        category = 'aggregators'
        description = config.Arguments.args[category]['description']
        aggregator_group = self.parser.add_argument_group(
            category.title(), description
        )
        # BUG: http://bugs.python.org/issue25882
        # for us it worked.
        exclusive_group = aggregator_group.add_mutually_exclusive_group()
        for arg in config.Arguments.args[category]['arguments']:
            exclusive_group.add_argument('--' + arg[0], '-' + arg[1], help=arg[2], action=arg[3])

    def add_arguments(self):
        """ Add all the arguments to the parser.
        """
        self._add_filter_args()
        self._add_aggregator_args()
        self._add_dir_arg()

    def _get_filter_args(self):
        """ Get the filter args from user input.
        """
        category = 'filters'
        self.filter_args = [
            (arg[0], getattr(self.namespace, arg[0]))
            for arg in config.Arguments.args[category]['arguments']
        ]

    def _get_aggregator_args(self):
        """ Get the aggregator args from user input.
        """
        category = 'aggregators'
        self.aggregator_args = [
            (arg[0], getattr(self.namespace, arg[0]))
            for arg in config.Arguments.args[category]['arguments']
        ]

    def _get_dir_arg(self):
        """ Get the filter args from user input.
        """
        self.logs_dir = self.namespace.logs_dir

    def _fix_dir_arg(self):
        """ Modify directory arg. if required
        """
        self.logs_dir = os.path.abspath(self.logs_dir)

    def _fix_filter_args(self):
        """ Fix filter ags if necessary.
        """
        for ind, (arg, val) in enumerate(self.filter_args):
            # IMP part, a bit too much happening, require explaining
            # if failed = True, remove this filter from the list
            # essentially it means collect all the records
            # (equivalent to no filter for success)
            # falied = false, means success = true (default filter)
            # and
            # use failed renders iff success count is in aggregators
            # Otherwise, if we have any other aggregation ex. max ram
            # then, failed renders don't have any info. about it.

            if arg == 'failed':
                if val and config.Columns.success in self.aggregator_args:
                    del(self.filter_args[ind])
                else:
                    self.filter_args[ind] = (config.Columns.success, 'true')
                break

        # TODO: if you have time use itemgetter please
        self.filter_args = filter(lambda x : bool(x[1]), self.filter_args)

    def _fix_aggregator_args(self):
        """ Modify aggregtor args. to suite our code.
        """
        has_summary = False
        for ind, (arg, val) in enumerate(self.aggregator_args):
            if all([arg == 'summary', val is True]):
                del(self.aggregator_args[ind])
                has_summary = True
                break

        if has_summary:
            for ind, (arg, val) in enumerate(self.aggregator_args):
                self.aggregator_args[ind] = arg
        else:
            for ind, (arg, val) in enumerate(self.aggregator_args):
                if val:
                    self.aggregator_args[ind] = arg
                else:
                    self.aggregator_args[ind] = None
            self.aggregator_args = filter(bool, self.aggregator_args)

        # if no aggregation type mentioned,
        # aggregate according to success (the default behaviour)
        if not self.aggregator_args:
            self.aggregator_args.append(config.Columns.success)

    def parse(self):
        """ Parse the user provided arguments.
        """
        self.namespace = self.parser.parse_args()

        self._get_dir_arg()
        self._get_filter_args()
        self._get_aggregator_args()

        self._fix_dir_arg()
        # order is imp. for below statements
        self._fix_aggregator_args()
        self._fix_filter_args()

    def display_output(self, output):
        """ Display fial output.

            Args:
                output (object) : final result received.
        """
        for arg, convert_func in config.Output.conversion.iteritems():
            try:
                index = self.aggregator_args.index(arg)
                output[index] = convert_func(output[index])
            except ValueError:
                pass
        # TODO: in future take diffent output streams: stdout, file, to db etc.
        output = [str(o) for o in output]
        print config.Output.display_delimiter.join(output)
