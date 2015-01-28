/*
Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You
may not use this file except in compliance with the License. A copy of
the License is located at

http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package com.amazonaws.ecs.sample;

import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.ContainerInstance;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesRequest;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesResult;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.ListContainerInstancesRequest;
import com.amazonaws.services.ecs.model.ListContainerInstancesResult;
import com.amazonaws.services.ecs.model.Resource;
import com.amazonaws.services.ecs.model.StartTaskRequest;
import com.amazonaws.services.ecs.model.StartTaskResult;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import com.amazonaws.services.ecs.model.StopTaskResult;
import com.amazonaws.services.ecs.model.Task;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ECSSchedulerDriver implements SchedulerDriver {
    private static final String MESOS_CPU = "cpus";
    private static final String MESOS_MEM = "mem";
    private static final String MESOS_PORTS = "ports";
    private static final String MESOS_DISK = "disk";

    private static final String AWS_ECS_CPU = "CPU";
    private static final String AWS_ECS_MEM = "MEMORY";
    private static final String AWS_ECS_PORTS = "PORTS";

    private static final Integer AWS_ECS_MAX_PORT = 60000;
    private static final int OFFER_REFRESH_RATE = 60;

    // An object to use for synchronization.
    private final Object lock = new Object();

    private final Scheduler scheduler;
    private final Map<Protos.TaskID, String> taskMap;
    private final AmazonECSClient client;
    private final String clusterName;
    private final Protos.MasterInfo masterInfo;
    private final ScheduledExecutorService executorService;

    private Protos.FrameworkInfo frameworkInfo;
    private Protos.Status status;

    public ECSSchedulerDriver(final Scheduler scheduler,
                              final Protos.FrameworkInfo frameworkInfo,
                              final String masterName) {
        //Probably would be better to have a builder for this class so that all of these could be injected properly
        this.status = Protos.Status.DRIVER_NOT_STARTED;
        this.scheduler = scheduler;
        if (frameworkInfo.getId().isInitialized()) {
            this.frameworkInfo = frameworkInfo;
        } else {
            this.frameworkInfo = Protos.FrameworkInfo.newBuilder(frameworkInfo)
                    .setId(Protos.FrameworkID.newBuilder()
                                    .setValue(UUID.randomUUID().toString())
                                    .build()
                    )
                    .build();
        }
        this.taskMap = new HashMap<Protos.TaskID, String>();
        this.client = new AmazonECSClient();
        String endpoint = System.getenv("AWS_ECS_ENDPOINT");
        if (endpoint == null || "".equals(endpoint)) {
            endpoint = "https://ecs.us-east-1.amazonaws.com";
        }
        client.setEndpoint(endpoint);
        String cluster = System.getenv("AWS_ECS_CLUSTER");
        if (cluster == null || "".equals(cluster)) {
            cluster = "default";
        }
        this.clusterName = cluster;
        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setHostname(endpoint)
                .setId(endpoint + masterName + cluster)
                .setIp(0)
                .setPort(443)
                .build();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public Protos.Status start() {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_NOT_STARTED) {
                return status;
            }

            scheduler.registered(this, frameworkInfo.getId(), masterInfo);
            executorService.scheduleWithFixedDelay(new ResourceGenerator(), OFFER_REFRESH_RATE, OFFER_REFRESH_RATE, TimeUnit.SECONDS);
            status = Protos.Status.DRIVER_RUNNING;
            lock.notifyAll();
            return status;
        }
    }

    @Override
    public Protos.Status stop(boolean b) {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_RUNNING && status != Protos.Status.DRIVER_ABORTED) {
                return status;
            }
            executorService.shutdown();
            status = Protos.Status.DRIVER_STOPPED;
            lock.notifyAll();
            return status;
        }
    }

    @Override
    public Protos.Status stop() {
        return stop(false);
    }

    @Override
    public Protos.Status abort() {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_RUNNING) {
                return status;
            }

            status = Protos.Status.DRIVER_ABORTED;
            lock.notifyAll();
            return status;
        }
    }

    @Override
    public Protos.Status join() {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_RUNNING) {
                return status;
            }

            while (status == Protos.Status.DRIVER_RUNNING) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    // Just keep going.
                }
            }
            return status;
        }
    }

    @Override
    public Protos.Status run() {
        Protos.Status localStatus = start();
        if (localStatus != Protos.Status.DRIVER_RUNNING) {
            return status;
        } else {
            return join();
        }
    }

    @Override
    public Protos.Status requestResources(Collection<Protos.Request> collection) {
        // Noop, we always offer all resources, this could be improved.
        synchronized (lock) {
            return status;
        }
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIDs, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        // We completely ignore filters, this is an area for optimization but not needed since we expose full cluster state.
        // Declines(offerIds with no tasks) are also ignored as they're not needed with Amazon ECS state management.
        synchronized (this) {
            if (offerIDs.isEmpty()) {
                return status;
            }

            // The Amazon ECS apis only allow 10 container instances per StartTask call, so partition.
            final List<String> containerInstanceIDs = new ArrayList<String>(offerIDs.size());
            for (final Protos.OfferID offerID : offerIDs) {
                containerInstanceIDs.add(offerID.getValue());
            }
            final List<List<String>> containerInstancePartitions = partition(containerInstanceIDs, 10);

            for (final Protos.TaskInfo taskInfo : tasks) {
                final StartTaskRequest request = new StartTaskRequest();
                request.setCluster(clusterName);
                // Mesos's data model does not match with the Amazon ECS data model.
                // Amazon ECS interprets "command" to be the already-registered TaskDefinition.
                request.setTaskDefinition(taskInfo.getCommand().getValue());
                for (final List<String> containerInstances : containerInstancePartitions) {
                    request.setContainerInstances(containerInstances);
                    final StartTaskResult result = client.startTask(request);
                    for (final Task ecsTask : result.getTasks()) {
                        taskMap.put(taskInfo.getTaskId(), ecsTask.getTaskArn());
                        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                                .setTaskId(taskInfo.getTaskId())
                                .setSlaveId(Protos.SlaveID.newBuilder().setValue(ecsTask.getContainerInstanceArn()))
                                .setState(Protos.TaskState.TASK_RUNNING)
                                .build();
                        scheduler.statusUpdate(this, taskStatus);
                    }
                    for (final Failure failure : result.getFailures()) {
                        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                                .setTaskId(taskInfo.getTaskId())
                                .setSlaveId(Protos.SlaveID.newBuilder().setValue(failure.getArn()))
                                .setState(Protos.TaskState.TASK_FAILED)
                                .build();
                        scheduler.statusUpdate(this, taskStatus);
                    }
                }
            }
            return status;
        }
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIDs, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(offerIDs, tasks, Protos.Filters.newBuilder().build());
    }

    @Override
    public Protos.Status launchTasks(final Protos.OfferID offerID, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(new ArrayList<Protos.OfferID>() {{
            add(offerID);
        }}, tasks, filters);
    }

    @Override
    public Protos.Status launchTasks(Protos.OfferID offerID, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(offerID, tasks, Protos.Filters.newBuilder().build());
    }

    @Override
    public Protos.Status killTask(Protos.TaskID taskID) {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_RUNNING) {
                return status;
            }

            final String ecsTaskId = taskMap.get(taskID);
            if (ecsTaskId == null) {
                //TODO: Need to handle this WAY better, for now just do nothing?
                return status;
            }

            final StopTaskRequest request = new StopTaskRequest();
            request.setCluster(clusterName);
            request.setTask(ecsTaskId);
            final StopTaskResult result = client.stopTask(request);
            final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                    .setTaskId(taskID)
                    .setSlaveId(Protos.SlaveID.newBuilder().setValue(result.getTask().getContainerInstanceArn()))
                    .setState(Protos.TaskState.TASK_KILLED)
                    .build();
            scheduler.statusUpdate(this, taskStatus);
            return status;
        }
    }

    @Override
    public Protos.Status declineOffer(final Protos.OfferID offerID, Protos.Filters filters) {
        return launchTasks(new ArrayList<Protos.OfferID>() {{
            add(offerID);
        }}, new ArrayList<Protos.TaskInfo>(), filters);
    }

    @Override
    public Protos.Status declineOffer(Protos.OfferID offerID) {
        return declineOffer(offerID, Protos.Filters.newBuilder().build());
    }

    @Override
    public Protos.Status reviveOffers() {
        // Deliberate no-op
        synchronized (lock) {
            return status;
        }
    }

    @Override
    public Protos.Status sendFrameworkMessage(Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
        // Deliberate no-op
        synchronized (lock) {
            return status;
        }
    }

    @Override
    public Protos.Status reconcileTasks(Collection<Protos.TaskStatus> taskStatuses) {
        synchronized (lock) {
            if (status != Protos.Status.DRIVER_RUNNING) {
                return status;
            }

            final Map<String, Protos.TaskStatus> ecsTaskToStatus = new HashMap<String, Protos.TaskStatus>(taskStatuses.size());

            final List<String> taskIds = new ArrayList<String>(taskStatuses.size());
            for (final Protos.TaskStatus taskStatus : taskStatuses) {
                final String ecsTaskId = taskMap.get(taskStatus.getTaskId());
                if (ecsTaskId == null) {
                    //TODO: persist this properly so we don't get here.
                    continue;
                }
                taskIds.add(ecsTaskId);
                ecsTaskToStatus.put(ecsTaskId, taskStatus);
            }
            // The Amazon ECS APIs only allow 100 tasks per DescribeTasks call, so partition.
            final List<List<String>> taskIdsPartition = partition(taskIds, 100);

            for (final List<String> tasks : taskIdsPartition) {
                final DescribeTasksRequest request = new DescribeTasksRequest();
                request.setCluster(clusterName);
                request.setTasks(tasks);
                final DescribeTasksResult result = client.describeTasks(request);
                for (final Task task : result.getTasks()) {
                    if ("STOPPED".equals(task.getLastStatus())) {
                        final Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                                .mergeFrom(ecsTaskToStatus.get(task.getTaskArn()))
                                .setState(Protos.TaskState.TASK_FINISHED)
                                .build();
                        scheduler.statusUpdate(this, taskStatus);
                    }
                }
            }
            return status;
        }
    }

    private <T> List<List<T>> partition(final List<T> toPartition, int perPartition) {
        final List<List<T>> finalPartitions = new ArrayList<List<T>>();
        List<T> partition = new ArrayList<T>();
        Iterator<T> toPartitionIterator = toPartition.iterator();
        for (int i = 0; i < toPartition.size(); i++) {
            if (!toPartitionIterator.hasNext()) {
                break;
            }

            if (i % perPartition == 0) {
                partition = new ArrayList<T>(perPartition);
                finalPartitions.add(partition);
            }

            partition.add(toPartitionIterator.next());
        }

        return finalPartitions;
    }

    private class ResourceGenerator implements Runnable {
        @Override
        public void run() {
            synchronized (lock) {
                try {
                    final ListContainerInstancesRequest listRequest = new ListContainerInstancesRequest();
                    listRequest.setCluster(clusterName);

                    final List<ContainerInstance> instances = new ArrayList<ContainerInstance>();
                    for (; ; ) {
                        final ListContainerInstancesResult listResult = client.listContainerInstances(listRequest);
                        final DescribeContainerInstancesRequest describeRequest = new DescribeContainerInstancesRequest();
                        describeRequest.setCluster(clusterName);
                        describeRequest.setContainerInstances(listResult.getContainerInstanceArns());
                        final DescribeContainerInstancesResult describeResult = client.describeContainerInstances(describeRequest);
                        instances.addAll(describeResult.getContainerInstances());
                        if (listResult.getNextToken() == null) {
                            break;
                        }
                        listRequest.setNextToken(listResult.getNextToken());
                    }

                    final List<Protos.Offer> offers = new ArrayList<Protos.Offer>(instances.size());
                    for (final ContainerInstance ci : instances) {
                        if (!ci.isAgentConnected()) {
                            // If the agent isn't connected StartTask won't work
                            continue;
                        }

                        final Protos.Offer.Builder offerBuilder = Protos.Offer.newBuilder()
                                .setHostname(ci.getContainerInstanceArn())
                                .setFrameworkId(frameworkInfo.getId())
                                .setId(Protos.OfferID.newBuilder().setValue(ci.getContainerInstanceArn()).build())
                                .setSlaveId(Protos.SlaveID.newBuilder().setValue(ci.getContainerInstanceArn()).build());

                        offerBuilder.addResources(
                                Protos.Resource.newBuilder()
                                        .setType(Protos.Value.Type.SCALAR)
                                        .setName(MESOS_DISK) // Currently not supported in Amazon ECS
                                        .setRole("*")
                                        .setScalar(
                                                Protos.Value.Scalar.newBuilder()
                                                        .setValue(0.0)
                                                        .build()
                                        )
                        );

                        for (final Resource resource : ci.getRemainingResources()) {
                            if (AWS_ECS_CPU.equals(resource.getName())) {
                                offerBuilder.addResources(
                                        Protos.Resource.newBuilder()
                                                .setType(Protos.Value.Type.SCALAR)
                                                .setName(MESOS_CPU)
                                                .setRole("*")
                                                .setScalar(
                                                        Protos.Value.Scalar.newBuilder()
                                                                .setValue(resource.getIntegerValue().doubleValue() / 1024.0) // CPU is a normalized int, divide by 1024 to get to what Mesos expects
                                                                .build()
                                                )
                                );
                            } else if (AWS_ECS_MEM.equals(resource.getName())) {
                                offerBuilder.addResources(
                                        Protos.Resource.newBuilder()
                                                .setType(Protos.Value.Type.SCALAR)
                                                .setName(MESOS_MEM)
                                                .setRole("*")
                                                .setScalar(
                                                        Protos.Value.Scalar.newBuilder()
                                                                .setValue(resource.getIntegerValue().doubleValue())
                                                                .build()
                                                )
                                );
                            } else if (AWS_ECS_PORTS.equals(resource.getName())) {
                                offerBuilder.addResources(
                                        Protos.Resource.newBuilder()
                                                .setType(Protos.Value.Type.RANGES)
                                                .setName(MESOS_PORTS)
                                                .setRole("*")
                                                .setRanges(portRanges(resource))
                                );
                            }
                        }
                        offers.add(offerBuilder.build());
                    }

                    scheduler.resourceOffers(ECSSchedulerDriver.this, offers);
                } catch (Throwable e) {
                    // Keep this thread running no matter what
                }
            }
        }

        private Protos.Value.Ranges portRanges(final Resource portResource) {
            final Protos.Value.Ranges.Builder rangeBuilder = Protos.Value.Ranges.newBuilder();
            final List<Integer> reservedPorts = new ArrayList<Integer>();
            //Bottom end of any port range is 0 + 1.  We add it manually to get things started.
            reservedPorts.add(0);
            for (final String port : portResource.getStringSetValue()) {
                reservedPorts.add(Integer.valueOf(port));
            }
            // We need the list of reserved ports sorted to properly turn them into the Mesos expected port ranges.
            Collections.sort(reservedPorts);
            for (int i = 0; i < reservedPorts.size(); i++) {
                Integer realStart = reservedPorts.get(i) + 1;
                // Don't allow port usage past the maximum
                Integer realEnd = AWS_ECS_MAX_PORT;
                if (i + 1 < reservedPorts.size()) {
                    realEnd = Math.min(reservedPorts.get(i + 1) - 1, realEnd);
                }
                if (realEnd >= realStart) {
                    rangeBuilder.addRange(
                            Protos.Value.Range.newBuilder()
                                    .setBegin(realStart.longValue())
                                    .setEnd(realEnd.longValue())
                                    .build()
                    );
                }
            }
            return rangeBuilder.build();
        }
    }
}
