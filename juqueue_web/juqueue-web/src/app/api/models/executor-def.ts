/* tslint:disable */
/* eslint-disable */
export interface ExecutorDef {
  cuda?: boolean;
  env?: {
[key: string]: string;
};
  prepend_script?: Array<string>;
  python_search_path?: Array<string>;
  venv?: string;
}
