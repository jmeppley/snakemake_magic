"""
Defines magic functions to run snakemake from within ipython:

The current approach is to create a snakemake.workflow object the first time a cell
magic is called. The contents of the cell are put into a temporary file and loaded
into the workflow by using worfklow.include() or workflow.config.update().

"""
from __future__ import print_function
import os
import re
import tempfile
import shlex
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic)

from snakemake.workflow import Workflow
import snakemake.workflow
from snakemake.io import load_configfile, _load_configfile
from snakemake.parser import parse
from snakemake import get_argument_parser, parse_resources, logger

rule_rexp = re.compile(r'^\s*@workflow.rule\s*\(name=\s*\'([^\']+)\'')
def get_rule_names(snakefile_name):
    """ Run snakemake.parser.parse and use regexp to get the rule names
        This is incredibly inefficient, but a little more stable than trying to
        parse rules myself.
    """
    for line in parse(snakefile_name)[0].split("\n"):
        m = rule_rexp.match(line)
        if m:
            yield m.group(1)

# To register magic, class MUST call this class decorator at creation time
@magics_class
class SnakemakeMagic(Magics):
    """
    Defines two cell and one line magic:

     * %%config: load a block of yaml or json into the config object
     * %%include: load a block of snakefile code into the workflow
     * %smake: attempt to create a target file
    """
    workflow = None
    tempfiles = {"cells":[]}
    updated_rules = []

    # TODO: add magic to reset workflow with more options
    #  EG: cluster script, threads, ...

    def get_workflow(self):
        """ make sure there is a workflow object

        TODO:
            * allow multiple workflows?
            * what king of options to allow?
            * allow options every time or just first?
        """
        if self.workflow is None:
            # create a new workflow object with some basic defaults

            # create a blank file just so snakemake has something to hang on to
            # (this file cannot be read from on some Windows systems...)
            self.tempfiles['root'] = tempfile.NamedTemporaryFile('w')
            self.workflow = Workflow(snakefile=self.tempfiles['root'].name)

        return self.workflow
    
    @line_magic
    def snakemake(self, line):
        """ execute the workflow with the given arguments and targets
        
        This uses the snakeamke command line argument parser for now.
        """
        if self.workflow is None:
            raise Exception("Workflow has no data!")

        parser = get_argument_parser()
        args = parser.parse_args(list(shlex.split(line)))

        logger.debug(repr(args))
        
        resources = parse_resources(args)

        targets = args.target
        dryrun = args.dryrun
        printshellcmds = args.printshellcmds
        printreason = args.reason
        printrulegraph = args.rulegraph
        printd3dag = args.d3dag
        touch = args.touch
        forceall = args.forceall
        forcerun = set(args.forcerun if args.forcerun is not None else [] + self.updated_rules)
        prioritytargets = args.prioritize
        until = args.until
        omit_from = args.omit_from
        stats = args.stats
        nocolor = args.nocolor
        quiet = args.quiet
        keepgoing = args.keep_going
        standalone = True
        ignore_ambiguity = args.allow_ambiguity
        lock = not args.nolock
        unlock = args.unlock
        force_incomplete = args.rerun_incomplete
        ignore_incomplete = args.ignore_incomplete
        list_version_changes = args.list_version_changes
        list_code_changes = args.list_code_changes
        list_input_changes = args.list_input_changes
        list_params_changes = args.list_params_changes
        summary = args.summary
        detailed_summary = args.detailed_summary
        print_compilation = args.print_compilation
        verbose = args.verbose
        debug = args.debug
        notemp = args.notemp
        keep_remote_local = args.keep_remote
        greediness = args.greediness
        latency_wait = args.latency_wait
        benchmark_repeats = args.benchmark_repeats
        keep_target_files = args.keep_target_files

        updated_files = list()

        if greediness is None:
            greediness = 0.5 if prioritytargets else 1.0
        else:
            if not (0 <= greediness <= 1.0):
                logger.error("Error: greediness must be a float between 0 and 1.")
                return False

        # TODO: set target, check workflow, execute workflow
        workflow = self.get_workflow()
        # TODO: keep track of updated rules to set force run

        # HACK: execute() is leaving directory locked, so I'm disabling locks
        lock = False
        
        workflow.check()

        success = workflow.execute(
            targets=targets,
            dryrun=dryrun,
            touch=touch,
            forceall=forceall,
            forcerun=forcerun,
            until=until,
            omit_from=omit_from,
            quiet=quiet,
            keepgoing=keepgoing,
            printshellcmds=printshellcmds,
            printreason=printreason,
            printrulegraph=printrulegraph,
            printd3dag=printd3dag,
            ignore_ambiguity=ignore_ambiguity,
            stats=stats,
            force_incomplete=force_incomplete,
            ignore_incomplete=ignore_incomplete,
            list_version_changes=list_version_changes,
            list_code_changes=list_code_changes,
            list_input_changes=list_input_changes,
            list_params_changes=list_params_changes,
            summary=summary,
            latency_wait=latency_wait,
            benchmark_repeats=benchmark_repeats,
            wait_for_files=None,
            detailed_summary=detailed_summary,
            nolock=not lock,
            unlock=unlock,
            notemp=notemp,
            keep_remote_local=keep_remote_local,
            keep_target_files=keep_target_files,
            updated_files=updated_files,
            resources=resources,
            )
        
        if success:
            del self.updated_rules[:]
        
        return success

    @cell_magic
    def sinclude(self, line, cell):
        "include this cell in workflow"

        workflow = self.get_workflow()

        # snakemake does not support blocks of text, so we create a temp
        #  file.
        cell_snakefile = tempfile.NamedTemporaryFile('w', delete=False)
        self.tempfiles['cells'].append(cell_snakefile.name)
        cell_snakefile.write(cell)
        cell_snakefile.close()
        
        # first rule is first rule
        overwrite_first_rule = len(workflow._rules) == 0

        # HACK: remove rules to be replaced:
        # remove conflicting rules
        for rule_name in get_rule_names(cell_snakefile.name):
            workflow._rules.pop(rule_name, None)
            self.updated_rules.append(rule_name)

        # include snippet
        workflow.include(cell_snakefile.name,
                         overwrite_first_rule=overwrite_first_rule,
                        )
        os.unlink(cell_snakefile.name)

        return "Workflow now has {} rules".format(len(workflow._rules))

    @cell_magic
    def sconfig(self, line, cell):
        " Load JSON or YAML into workflow's config object "
        workflow = self.get_workflow()

        # create a temp file, so we can use snakemake.load_configfile
        #  it wouldn't be hard to roll our own to avoid this...
        cell_config_file = tempfile.NamedTemporaryFile('w', delete=False)
        cell_config_file.write(cell)
        cell_config_file.close()
        
        snakemake.workflow.config.update(load_configfile(cell_config_file.name))
        logger.debug(repr(snakemake.workflow.config))
        os.unlink(cell_config_file.name)

    @line_magic
    def _workflow(self, line):
        "backdoor to inspect workflow object"
        # this wouldn't be in the final version
        return self.get_workflow()
    
    @line_magic
    def _reset_workflow(self, line):
        self.workflow = None

# In order to actually use these magics, you must register them with a
# running IPython.  This code must be placed in a file that is loaded once
# IPython is up and running:
ip = get_ipython()
# You can register the class itself without instantiating it.  IPython will
# call the default constructor on it.
ip.register_magics(SnakemakeMagic)
