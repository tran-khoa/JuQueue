/* tslint:disable */
/* eslint-disable */
import { NodeInfo } from './node-info';
export interface ClusterInfo {
  nodes: {
[key: string]: NodeInfo;
};
  nodes_requested: number;
}
