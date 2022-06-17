import { Component, OnInit } from '@angular/core';
import { ClusterInfo } from '../api/models';
import { DataService } from '../data.service';
import { ToastService } from '../toast.service';

@Component({
  selector: 'app-cluster-list',
  templateUrl: './cluster-list.component.html',
  styleUrls: ['./cluster-list.component.scss']
})
export class ClusterListComponent implements OnInit {
  clusters: {[key: string]: ClusterInfo;} = {};

  constructor(
    private dataService: DataService,
    private toastService: ToastService) { }

  ngOnInit(): void {
    this.dataService.postClusterUpdateCallbacks.push((clusters) => this.clusters = clusters);
    //this.appComponent.registerPreUpdateCallback((experiments) => this.onPreUpdate(experiments));
    //this.appComponent.registerPostUpdateCallback((experiments) => this.onPostUpdate(experiments));
  }

  reloadClusterDefs() {
    this.dataService.juqueueService.reloadClusters().subscribe((resp) => {
      if (resp.success) {
        this.toastService.showSuccess("Successfully reloaded cluster definitions.");
      } else if (resp.reason !== undefined) {
        this.toastService.showError(resp.reason)
      } else {
        this.toastService.showError("An unspecified error occured.")
      }
    });
  }
}
