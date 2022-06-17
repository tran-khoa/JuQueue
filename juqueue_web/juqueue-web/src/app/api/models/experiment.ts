/* tslint:disable */
/* eslint-disable */
import { Run } from './run';

/**
 * Logical group of runs
 */
export interface Experiment {

  /**
   * List of runs associated with this experiment.
   */
  runs: {
[key: string]: Run;
};
}
