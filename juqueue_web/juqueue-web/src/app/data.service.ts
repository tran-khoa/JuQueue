import { Injectable, isDevMode } from '@angular/core';
import { forkJoin } from 'rxjs';
import { Experiment, ClusterInfo } from './api/models';
import { ClusterService, ExperimentService, JuQueueService } from './api/services';
import { WebsocketService } from './websocket.service';


type ExperimentData = {[key: string]: Experiment;};
type ClusterData = {[key: string]: ClusterInfo;}



@Injectable({
  providedIn: 'root'
})
export class DataService {

  rootUrl: string;
  experiments: ExperimentData = {};
  clusters: ClusterData = {};
  lastUpdated: Date|null = null;

  preExperimentUpdateCallbacks: ((experiments: ExperimentData) => void)[] = [];
  postExperimentUpdateCallbacks: ((experiments: ExperimentData) => void)[] = [];
  preClusterUpdateCallbacks: ((clusters: ClusterData) => void)[] = [];
  postClusterUpdateCallbacks: ((clusters: ClusterData) => void)[] = [];
  postUpdateCallbacks: (() => void)[] = [];


  constructor(
    public experimentService: ExperimentService,
    public clusterService: ClusterService,
    public juqueueService: JuQueueService,
    private socketService: WebsocketService,
  ) {
    if (isDevMode()) {
      this.rootUrl = 'http://localhost:8080';
    } else {
      this.rootUrl = '.';
    }

    experimentService.rootUrl = this.rootUrl;
    clusterService.rootUrl = this.rootUrl;
    juqueueService.rootUrl = this.rootUrl;

    this.fetch();
    this.socketService.register_callback(() => this.fetch());
  }

  fetch(done_callback: (() => void)|null = null) {
    const experimentsObs = this.fetchExperiments();
    const clustersObs = this.fetchClusters();
    
    forkJoin([experimentsObs, clustersObs]).subscribe(() => {
      this.lastUpdated = new Date();

      if (done_callback !== null)
        done_callback()
      
      this.postUpdateCallbacks.forEach(cb => cb());
    });
  }

  fetchExperiments() {
    const obs = this.experimentService.getExperiments()
    obs.subscribe(
      newExperimentData => {
        this.preExperimentUpdateCallbacks.forEach(cb => cb(newExperimentData));
        this.experiments = newExperimentData;
        this.postExperimentUpdateCallbacks.forEach(cb => cb(newExperimentData));        
      }
    );
    return obs;
  }

  fetchClusters() {
    const obs = this.clusterService.getClusters()
    obs.subscribe(
      newClusterData => {
        this.preClusterUpdateCallbacks.forEach(cb => cb(newClusterData));
        this.clusters = newClusterData;
        this.postClusterUpdateCallbacks.forEach(cb => cb(newClusterData));
      }
    )
    return obs;
  }
}
