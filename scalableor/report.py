

class Report:

    def __init__(self, output_file):

        self.output_file = output_file
        self.op_errors = []
        self.row_errors = []

    def op_error(self, operation, error):
        self.op_errors.append((operation, error))

    def row_error(self, operation, error, row):
        self.op_errors.append((operation, error, row))

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
                                  .format(len(self.row_errors), len(self.op_errors)))

                if len(self.op_errors) > 0:
                    output_file.writelines([
                        "Operation-specific errors:\n",
                        "--------------------------\n"
                    ])

                    output_file.writelines(["{}, in Operation '{}'.\n".format(msg, op) for op, msg in self.op_errors])