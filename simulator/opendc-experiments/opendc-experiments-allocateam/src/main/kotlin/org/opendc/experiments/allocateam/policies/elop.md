# ELOP logic

Explanation of ELoP from [this paper @ section III D (The Strict Reservation policy)](http://pure.tudelft.nl/ws/portalfiles/portal/38016300/CCGrid2015_ASIlyushkin).

> LOP = Level of Parallelism


For each scheduling cycle:


In T3 (resourceSelectionPolicy):

1. Compute LOP for DAG of WF.
1. If LOP fits in number of `available` processors, allocate and reserve these processors.
1. IF LOP does NOT fit in number of `available` processors, allocate and reserve whatever is available.
1. If no resources are `available`, i.e. if all resources are reserved, admit no new WFs.


# Old logic:

```
In J2 (Job Admission):

1. Compute LOP for (sub)DAG of WF.
1. If LOP fits in number of `available` processors, allocate and reserve these processors.
1. IF LOP does NOT fit in number of `available` processors, allocate and reserve whatever is available.
1. If no resources are `available`, i.e. if all resources are reserved, admit no new WFs.
```
