# -*- coding: utf-8 -*-

from scalableor.manager import VerifiersManager


@VerifiersManager.register("scalableor/import")
def core_column_split(cmd, errors):
    required_params = ["separator", "path"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("scalableor/export")
def core_column_split(cmd, errors):
    required_params = ["path"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/column-rename")
def core_column_split(cmd, errors):
    required_params = ["oldColumnName", "newColumnName"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/column-removal")
def core_column_split(cmd, errors):
    required_params = ["columnName"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/column-move")
def core_column_split(cmd, errors):
    required_params = ["columnName", "index"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/column-split")
def core_column_split(cmd, errors):
    intern_error = []
    required_params_list = [["separator", "columnName"], ["fieldLengths", "columnName"]]
    for required_params in required_params_list:
        if None in [cmd.get(i) for i in required_params]:
            intern_error.append("Required parameter is undefined. List of required parameters: %s" % required_params)

    if len(intern_error) == len(required_params_list):
        map(errors.append, intern_error)


@VerifiersManager.register("core/column-addition")
def core_column_split(cmd, errors):
    required_params = ["baseColumnName", "expression", "newColumnName"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/text-transform")
def core_column_split(cmd, errors):
    required_params = ["columnName", "expression"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/mass-edit")
def core_column_split(cmd, errors):
    required_params = ["columnName", "edits"]
    if None in [cmd.get(i) for i in required_params]:
        errors.append("Required parameter is undefined. List of required parameters: %s" % required_params)


@VerifiersManager.register("core/row-removal")
def core_column_split(cmd, errors):
    if not cmd.get("engineConfig", {}).get("facets", []):
        errors.append("Required parameter is undefined. List of required parameters: engineConfig.facets")
