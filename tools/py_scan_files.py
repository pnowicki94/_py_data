# -*- coding: utf-8 -*-
import pandas as pd
import os


class ScanFiles:

    def __init__(self, path_dir_scan, path_report):

        self.path_dir_scan = path_dir_scan
        self.path_report = path_report

        self.df_scan = pd.DataFrame(columns=['path_to_file', 'filename', 'extension'])

    def scan_files(self, path_current):

        for item in os.listdir(path_current):

            if os.path.isfile(os.path.join(path_current, item)):
                path_report = path_current.replace(self.path_dir_scan, '')

                extension = item.split('.')[-1]
                dane = {'path_to_file': path_report[1:],
                        'filename': item,
                        'extension': extension,
                        }
                self.df_scan = self.df_scan.append(dane, ignore_index=True)

            elif os.path.isdir(os.path.join(path_current, item)):

                self.scan_files(os.path.join(path_current, item))

    def generate_report(self):
        print(self.generate_report.__name__)
        self.df_scan.to_excel(self.path_report)

    def __repr__(self):
        return f"{type(self).__name__}"


class CompareScanData:

    def __init__(self, path_scan, path_files, path_compare):
        df_scan = pd.read_excel(path_scan, header=0)
        df_scan = df_scan.rename(columns={'filename': 'filename_scan'}, inplace=False)

        df_scan['filename_scan_lower'] = df_scan['filename_scan'].str.lower()

        df_files = pd.read_excel(path_files, header=0)
        df_files = df_files.rename(columns={'filename': 'filename_files'}, inplace=False)

        df_files['filename_files_lower'] = df_files['filename_files'].str.lower()

        df_compare = pd.merge(df_files, df_scan,
                              left_on=['filename_files_lower', 'rzgw_files'],
                              right_on=['filename_scan_lower', 'rzgw_scan'],
                              how='left')

        unnamed_cols = [c for c in list(df_compare.columns.values) if 'unnamed' in c.lower()]
        df_compare.drop(unnamed_cols + ['filename_files_lower', 'filename_scan_lower'], axis='columns', inplace=True)

        df_compare.to_excel(path_compare)
