/* tslint:disable */
/* eslint-disable */
import { ExecutorDef } from './executor-def';

/**
 * Defines the execution of a run.
 */
export interface RunDef {

  /**
   * Not implemented yet.
   */
  check_heartbeat?: boolean;

  /**
   * The cluster this run should run on, as defined in clusters.yaml.
   */
  cluster: string;

  /**
   * The command to be executed, as a list.
   */
  cmd: Array<string>;

  /**
   * Not implemented yet.
   */
  depends_on?: Array<RunDef>;

  /**
   * Specifies the execution environment the command is run in. Refer to ExecutorDef.
   */
  executor?: ExecutorDef;

  /**
   * The experiment this run belongs to.
   */
  experiment_name: string;

  /**
   * Uniquely identifies a run inside an experiment. Use global_id outside the scope of an experiment.
   */
  id: string;

  /**
   * Abstract runs are created as a template for other runs, but cannot be executed themselves.
   */
  is_abstract?: boolean;

  /**
   * 'argparse' formats the parameters as --key value, 'eq' as key=value.
   */
  parameter_format?: 'argparse' | 'eq';

  /**
   * Parameters appended to the specified command. Formatting determined by parameter_format.
   */
  parameters?: {
[key: string]: (string | number | number | boolean | any | string | Blob);
};
}
