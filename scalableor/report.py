import datetime

from scalableor.sampler import Sampler


class Report:

    def __init__(self, output_file, input_path, program_path):
        """ Creates a new, empty report.

        :param output_file: (str) The path where the report will be saved to.
        :param input_path: (str) Path to the input file, necessary for the sample.
        """

        self.output_file = output_file
        self.input_path = input_path
        self.program_path = program_path
        self.op_errors = []
        self.row_errors = []

        # Create sample
        self.sample = Sampler(input_path)

    def op_error(self, operation, error):
        """ Adds an 'operation error' to the current report and appends the row to the sample.

        :param operation: (str) Name of the operation, e.g., 'core/column-remove'
        :param error: (str) Error message
        :param row: (list) The first row of the input file as example (the error occurs in every row).
        :return: None
        """

        """
        if type(row) is not list:
            print("Error: parameter 'row' is expected to be a list, {} given.".format(type(row)))
            return False
        """

        self.op_errors.append((operation, error))

        #self.sample.append(row)

    def row_error(self, operation, error, row, sample_append=True):
        """ Adds a 'row error' to the current report and appends the row to the sample.

        :param operation: (string) Name of the operation, e.g., 'core/column-split'
        :param error: (string) Error message
        :param row: (list) Row where the error occurred, one list element per column
        :param sample_append: (bool) Whether the row should be appended to the sample
        :return: None
        """

        if type(row) is not list:
            print("Error: parameter 'row' is expected to be a list, {} given.".format(type(row)))
            return False

        self.row_errors.append((operation, error, row))

        if sample_append:
            self.sample.append(row)

    def __del__(self):
        """ Saves the sample to a file. This is done in the destructor on purpose, so it does not have to be called
        manually.

        :return: None
        """

        # Save sample
        self.sample.save()

        # Write the report into a file when the class destructor is called
        with open(self.output_file, "w+") as output_file:

            i, o, p = self.input_path, self.output_file, self.program_path
            # Write header row
            output_file.writelines([
                "------------------------------------------------------------------------------------\n",
                "                            Scalable.OR Execution Report                            \n",
                "------------------------------------------------------------------------------------\n",
                "Input: " + i + "\n",
                "Output: " + o + "\n",
                "Program: " + p + "\n",
                "Date: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "\n\n"
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
