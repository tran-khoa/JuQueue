import { Component } from '@angular/core';
import { DataService } from './data.service';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  title = 'juqueue-web';

  lastUpdated: Date|null = null;


  constructor(
    private dataService: DataService,
    private toastService: ToastService,
  ) {
    this.dataService.postUpdateCallbacks.push(() => this.lastUpdated = this.dataService.lastUpdated);
  }

  refreshData() {
    this.dataService.fetch(() => this.toastService.showSuccess("Data refreshed."));
  }
}
