import { Component, OnInit } from '@angular/core';
import { Experiment } from '../api/models/experiment';
import { ToastService } from '../toast.service';
import { FormBuilder, FormControl, FormGroup } from '@angular/forms';
import { IMultiSelectOption, IMultiSelectSettings, IMultiSelectTexts } from 'ngx-bootstrap-multiselect';
import { filter_obj } from '../utils';
import { DataService } from '../data.service';

const RunStatusArray = ['running', 'ready', 'failed', 'inactive', 'finished'];

@Component({
  selector: 'app-experiment-list',
  templateUrl: './experiment-list.component.html',
  styleUrls: ['./experiment-list.component.scss']
})
export class ExperimentListComponent implements OnInit {

  experiments: {[key: string]: Experiment;} = {};
  experimentStatusCounts: {[key: string]: {[status: string]: number}} = {};

  cfg: {[key: string]: any} = {};
  open_panels: Set<string> = new Set();
  forms: {[key: string]: FormGroup} = {};

  run_status: IMultiSelectOption[];
  filters_status_settings: IMultiSelectSettings;
  filters_status_texts: IMultiSelectTexts;
  filters_status: {[key: string]: string[]} = {};

  constructor(
    private dataService: DataService,
    private toastService: ToastService, 
    private fb: FormBuilder,
    ) {
      this.run_status = [
        { id: "inactive", name: "inactive" },
        { id: "failed", name: "failed" },
        { id: "ready", name: "ready" },
        { id: "running", name: "running" },
        { id: "finished", name: "finished" },
      ];

      this.filters_status_settings = {
        buttonClasses: 'btn btn-outline-primary',
        containerClasses: 'ms-2 dropdown-inline',
        showCheckAll: true,
        showUncheckAll: true,
        displayAllSelectedText: true,
        minSelectionLimit: 1,
        dynamicTitleMaxItems: 5
      };

      this.filters_status_texts = {
        checkAll: 'Select all',
        uncheckAll: 'Unselect all',
        checked: 'item selected',
        checkedPlural: 'items selected',
        defaultTitle: 'Filter by status',
        allSelected: 'Any status',
      };
    }

  /*
  Data logic
  */
  
  ngOnInit(): void {
    this.dataService.preExperimentUpdateCallbacks.push((experiments) => this.onPreUpdate(experiments));
    this.dataService.postExperimentUpdateCallbacks.push((experiments) => this.onPostUpdate(experiments));
  }

  onPreUpdate(experiments: {[key: string]: Experiment;}): void {

    // Update panel states
    this.open_panels = new Set([...this.open_panels].filter(
      (xp_key: string) => xp_key in experiments
    ));

    // Update filter status
    this.filters_status = filter_obj(this.filters_status, (key) => key in experiments);
    Object.keys(experiments).forEach((key) => {
      if (!(key in this.filters_status))
        this.filters_status[key] = [];
    });


    // Handle removed experiments
    const forms = filter_obj(this.forms, (key) => key in experiments);
    
    // Handle new experiments
    Object.keys(experiments).map((key: string) => {
      if (!(key in forms))
        forms[key] = this.fb.group({})
    })

    // Update experiments
    Object.keys(forms).forEach((xp_key: string) => {
      // Remove runs
      Object.keys(forms[xp_key]?.value)
            .filter((run_key: string) => !(run_key in experiments[xp_key].runs))
            .forEach((run_key: string) => forms[xp_key]?.removeControl(run_key));

      // Handle new runs
      Object.keys(experiments[xp_key].runs)
            .forEach((run_key: string) => {
              if (!(forms[xp_key]?.contains(run_key))) {
                forms[xp_key]?.addControl(run_key, new FormControl());
              }
            })
    });
    this.forms = forms;    

  }

  onPostUpdate(experiments: {[key: string]: Experiment;}): void {
    this.experimentStatusCounts = this.computeStatusCounts(experiments);
    this.experiments = experiments;
  }

  private computeStatusCounts(experiments: {[key: string]: Experiment;}): {[key: string]: {[status: string]: number}} {
    const counts: {[key: string]: {[status: string]: number}} = {};
    Object.entries(experiments).forEach(([key, xp]) => {
      counts[key] = Object.fromEntries(
        RunStatusArray.map(status => [status, 0])
      );

      Object.values(xp.runs).forEach(run => counts[key][run.status] += 1);
    });
    return counts;
  }

  /*
  View handling
  */

  panelOpened(xp_name: string) {
    this.open_panels.add(xp_name);
  }

  panelClosed(xp_name: string) {
    if (this.open_panels.has(xp_name))
      this.open_panels.delete(xp_name);
  }

  selectAll(experiment: string): void {
    document.querySelectorAll(`input[type='checkbox'][data-experiment='${experiment}']`).forEach(
      (el: Element) => {
        if (!(<HTMLInputElement>el).checked) {
          (<HTMLInputElement>el).click()
        }
      }
    )
  }

  selectInactive(experiment: string): void {
    document.querySelectorAll(`input[type='checkbox'][data-experiment='${experiment}']`).forEach(
      (el: Element) => {
        const run = this.experiments[experiment].runs[el.getAttribute("data-run")!];
        console.log(run);
        

        if (["inactive", "failed"].includes(run.status)) {
          if (!(<HTMLInputElement>el).checked) {
            (<HTMLInputElement>el).click()
          }
        } else {
          if ((<HTMLInputElement>el).checked) {
            (<HTMLInputElement>el).click()
          }
        }
      }
    )
  }

  selectNone(experiment: string): void {
    document.querySelectorAll(`input[type='checkbox'][data-experiment='${experiment}']`).forEach(
      (el: Element) => {
        if ((<HTMLInputElement>el).checked) {
          (<HTMLInputElement>el).click()
        }
      }
    )
  }

  getSelected(experiment: string): string[] {
    const result: string[] = Object.entries(this.forms[experiment]?.value)
      .filter(([, checked]) => checked)
      .map(([uid, ]) => uid);
    console.log("Selected runs: " + result);
    return result;
  }

  onFilterStatus(event: Event) {
  }

  onCheckboxChange(experiment: string) {
    const hasChecked = this.getSelected(experiment).length > 0;
    document.querySelectorAll(`button.run-action-batch[data-experiment='${experiment}']`).forEach(
      (el) => {
        if (hasChecked)
          (<HTMLButtonElement>el).disabled = false;
        else
        (<HTMLButtonElement>el).disabled = true;
      }
    );


  }


  /*
  Actions
  */

  resumeRuns(experiment: string, runs: string[]) {
    let obs = this.dataService.experimentService.resumeRuns(
      {experiment_name: experiment, body: runs}
    );
    obs.subscribe(
      (resp) => {
        if (resp.success) {
          this.toastService.showSuccess("Run(s) resumed.")
        } else if (resp.reason !== undefined) {
          this.toastService.showError(resp.reason)
        } else {
          this.toastService.showError("An unspecified error occured.")
        }
      }
    )
    return obs;
  }

  cancelRuns(experiment: string, runs: string[]) {
    let obs = this.dataService.experimentService.cancelRuns(
      {experiment_name: experiment, body: {
        run_ids: runs,
        force: true
      }}
    );
    obs.subscribe(
      (resp) => {
        if (resp.success) {
          this.toastService.showSuccess("Run(s) cancelled.")
        } else if (resp.reason !== undefined) {
          this.toastService.showError(resp.reason)
        } else {
          this.toastService.showError("An unspecified error occured.")
        }
      }
    )
    
    return obs;
  }
  
  resumeSelected(experiment: string): void {
    console.log("Resume selected: " + experiment);
    const selected: string[] = this.getSelected(experiment);
    this.resumeRuns(experiment, selected);
  }

  cancelSelected(experiment: string): void {
    console.log("Cancel selected: " + experiment);
    const selected: string[] = this.getSelected(experiment);
    this.cancelRuns(experiment, selected);
  }

  reloadExperimentDefs() {
    let obs = this.dataService.juqueueService.reloadExperiments();
    obs.subscribe(
      (resp) => {
        if (resp.success) {
          this.toastService.showSuccess("Successfully reloaded experiment definitions.");
        } else if (resp.reason !== undefined) {
          this.toastService.showError(resp.reason)
        } else {
          this.toastService.showError("An unspecified error occured.")
        }
      }
    )
    return obs;
  }
}
