from Pyro4 import expose
import math
import re

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers

    def solve(self):
        workersNumber = len(self.workers)
        agesArray = self.read_input()
        start = 0
        end = len(agesArray)

        interval_length_per_worker = len(agesArray) // workersNumber

        mapped = []
        for index in range(workersNumber):
            start = index * interval_length_per_worker
            end = start + interval_length_per_worker
            if index == workersNumber:
                end = len(agesArray)
            mapped.append(self.workers[index].mymap(agesArray[start:end]))

        result = self.myreduce(mapped)

        mean_age, confidence_interval_start, confidence_interval_end = self.calculate_statistics(result)
        
        agesArrayCheck = []
        for string in agesArray:
            parts = string.split()
            for part in parts:
                if part.isdigit():
                    agesArrayCheck.append(int(part))

        mean_age_check, confidence_interval_start_check, confidence_interval_end_check = self.calculate_statistics(agesArrayCheck)

        self.write_output(mean_age, mean_age_check, confidence_interval_start, confidence_interval_start_check, 
                          confidence_interval_end, confidence_interval_end_check, result)

    @staticmethod
    @expose
    def mymap(agesStringArray):
        agesArray = []
        for string in agesStringArray:
            parts = string.split()
            for part in parts:
                if part.isdigit():
                    agesArray.append(int(part))

        return sum(agesArray) / len(agesArray)

    @staticmethod
    @expose
    def myreduce(mapped):
        output = []
        for x in mapped:
            output.append(x.value)
        return output

    def read_input(self):
        with open(self.input_file_name, 'r') as file:
            ages = [line.strip() for line in file]
        return ages

    
    def calculate_statistics(self, result):
        mean_age = sum(result) / len(result)
        sample_standard_deviation = math.sqrt(sum((x - mean_age) ** 2 for x in result) / (len(result)) )
        margin_of_error = 1.96 * sample_standard_deviation / math.sqrt(len(result))
        confidence_interval_start = mean_age - margin_of_error
        confidence_interval_end = mean_age + margin_of_error
        return mean_age, confidence_interval_start, confidence_interval_end

    def write_output(self, mean_age, mean_age_check, confidence_interval_start, confidence_interval_start_check,
                    confidence_interval_end, confidence_interval_end_check, result):
        file = open(self.output_file_name, 'w')
        #file.write("result:"+ str(len(result)) + '\n')
        file.write("Mean Age: {:.2f}\n".format(mean_age))
        file.write("95percent Confidence Interval: {:.2f} - {:.2f}\n".format(confidence_interval_start, confidence_interval_end))
        file.write("Mean Age Check: {:.2f}\n".format(mean_age_check))
        file.write("95percent Confidence Interval Check: {:.2f} - {:.2f}\n".format(confidence_interval_start_check, confidence_interval_end_check))
        file.close()
