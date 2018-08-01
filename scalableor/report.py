

class Report:

    def __init__(self, output_file):

        self.output_file = output_file
        self.op_errors = []
        self.row_errors = []

    def op_error(self, operation, error):
        self.op_errors.append((operation, error))

    def row_error(self, operation, error, row):
        """ Adds a 'row error' to the current report.

        :param operation: (string) Name of the operation, e.g., 'core/column-split'
        :param error: (string) Error message
        :param row: (list) Row where the error occurred, one list element per column
        :return: None
        """

        if type(row) is not list:
            print("Error: parameter 'row' is expected to be a list, {} given.".format(type(row)))
            return False

        self.row_errors.append((operation, error, row))

    def __del__(self):

        # Write the report into a file when the class destructor is called
        with open(self.output_file, "w+") as output_file:

            # Write header row
            output_file.writelines([
                "------------------------------------------------------------------------------------\n",
                "                            Scalable.OR Execution Report                            \n",
                "------------------------------------------------------------------------------------\n\n"
            ])

            # Check if there were any errors
            if len(self.row_errors) + len(self.op_errors) == 0:
                output_file.write("No error occurred during the execution.")

            else:
                output_file.write("During the execution, {} operation-specific and {} row-specific errors occurred.\n\n"
                                  .format(len(self.op_errors), len(self.row_errors)))

                if len(self.op_errors) > 0:
                    output_file.writelines([
                        "Operation-specific errors:\n",
                        "--------------------------\n"
                    ])

                    output_file.writelines(["{}, in Operation '{}'.\n".format(msg, op) for op, msg in self.op_errors])

                if len(self.row_errors) > 0:
                    output_file.writelines([
                        "\nRow-specific errors:\n",
                        "--------------------\n"
                    ])

                    output_file.writelines(["{}, in Operation '{}'.\n{}\n".format(msg, op, ";".join(row))
                                            for op, msg, row in self.row_errors])
