
+ allocate 3 servers or VMs to setup k3s

    + (optional?) install proxmox in a few computers, make VMs so you can later tweak the cpu,ram specs.

    + hardware: 3 machines each with 12 cores 128 GB RAM (2 are used Lenovo ThinkStation P520)

    + one of the machines should have a large ssd (2TB+ ) for longhorn, later used by timescaledb.

    + one of the kube node should have a higher spect for timescaledb (8 core, 32GB)

+ install k3s HA with etcd: https://docs.k3s.io/datastore/ha-embedded

+ install longhorn: https://longhorn.io/docs/1.8.1/deploy/install/install-with-helm

    + test out longhorn, remember to create default storageclass

        + https://longhorn.io/docs/1.10.0/nodes-and-volumes/volumes/create-volumes

        + `kubectl create -f https://raw.githubusercontent.com/longhorn/longhorn/v1.10.0/examples/storageclass.yaml`

    + via longhorn web gui

        + setup 1 longhorn node with 1 volume using the large ssd.

        + for node, add tags: `storage`,`fast`

        + for volume, add tag: `ssd`

    + create storageclass.

        `kubectl apply -f db-storageclass.yaml`
