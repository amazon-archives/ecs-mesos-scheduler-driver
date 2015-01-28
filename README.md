# Amazon ECS Scheduler Driver

The Amazon EC2 Container Service (Amazon ECS) Scheduler Driver is an implementation of a Mesos Scheduler Driver which allows for direct
communication to Amazon ECS without the need for a Mesos Master or Mesos Slaves.

This allows for the use of existing Schedulers like Marathon and Chronos with Amazon ECS as the cluster
management solution.

*This is an example of what can be done with Amazon ECS, and is not recommended for production use.*

## Building
Run `mvn package` in the root directory, a jar will be produced in the target/ directory.

Alternatively, run `mvn install` to install the package to your local Maven repository.

## Integrating with a Mesos scheduler
If you followed the instructions to `mvn install` above, you should be able to modify the dependencies of your
project to depend on

```
<groupId>com.amazonaws</groupId>
<artifactId>amazon-ecs-scheduler-driver</artifactId>
<version>0.1</version>
```

If you are planning to use the .jar file instead, you'll need to ensure it is on the classpath of your Scheduler
and instead add an AWS SDK dependency to the scheduler.

Finally, you'll need to modify the instantiation of the `MesosSchedulerDriver` in whichever scheduler you're using
to instead instantiate the `ECSSchedulerDriver` instead.

## Using your Amazon ECS powered Mesos Scheduler
The ECSSchedulerDriver will interpret the `command` typically given when scheduling jobs with Mesos and instead
interpret this as your TaskDefinition family:revision.  For example, if Chronos or Marathon are given the `command=sleep360:1`,
a TaskDefinition which sleeps for 5 minutes and then exits, the ECSSchedulerDriver will issue commands to Amazon ECS
to run that family:revision.

The ECSSchedulerDriver expects the Task Definition to have been registered already.

### Declaring Resource Constraints(CPU, Memory, Ports)
Mesos and Amazon ECS account for resources differently.  With Mesos, CPU usage is a floating point number, so if you
wanted to run a task which used half of a CPU, you'd ask for cpu=0.5.  With Amazon ECS this is typically performed by
asking for cpu=512 instead as Amazon ECS gives each cpu 1024 units rather than 1.  With the ECSSchedulerDriver you
should declare your constraints with the Mesos style of 0.5 meaning half a cpu, and the ECSSchedulerDriver will convert
this for you.  Be aware, the TaskDefinition resource constraints should be equivalent to what you place in your scheduler.
For resources like memory, both systems count in megabytes, and for ports, you would declare the fixed ports that must
be reserved.

## A Marathon Example
After adding a dependency on the ECSSchedulerDriver and properly instantiating it instead of the MesosScheedulerDriver
as stated above, you can begin starting your Marathon managed Amazon ECS tasks.  An example Task Definition is below.

```
aws ecs describe-task-definition --task-definition nginx:2
{
    "taskDefinition": {
        "taskDefinitionArn": "arn:aws:ecs:us-east-1:***:task-definition/nginx:2",
        "containerDefinitions": [
            {
                "environment": [],
                "name": "nginx",
                "image": "nginx",
                "cpu": 100,
                "portMappings": [
                    {
                        "containerPort": 80,
                        "hostPort": 80
                    }
                ],
                "memory": 100,
                "essential": true
            }
        ],
        "family": "nginx",
        "revision": 2
    }
}
```

In order to run this command, you must use the new Docker portion of Marathon.  The Marathon UI, doesn't seem to support
this yet, so you'll use the command line and issue a command to the local Marathon.  The command
below will ask Marathon to keep 1 copy of the nginx:2 task running with matching resource constraints as what exists
in the Task Definition.  Once created, management of the scaling of this nginx task is done through the Marathon UI.

```
curl -i -H "Content-Type: application/json" -d '
{
  "id": "nginx",
  "cmd": "nginx:2",
  "cpus": 0.1,
  "mem": 100.0,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "ignore",
      "network": "BRIDGE",
      "portMappings": [
        {
          "containerPort":80,
          "hostPort":80
        }
      ]
    }
  }
}
' http://localhost:8080/v2/apps
```

## Additional Configuration
The following environment variables are available to configure your ECSSchedulerDriver.  Because it uses the
AWS SDK to communicate with Amazon ECS, any configuration for the SDK passed through key files or environment variables
can also be used.

| Environment Key | Example Value(s)            | Description | Default Value |
|:----------------|:----------------------------|:------------|:--------------|
| `AWS_ECS_CLUSTER`       | clusterName             | The cluster this ECSSchedulerDriver should manage. | default |
| `AWS_ECS_ENDPOINT` | https://ecs.us-east-1.amazonaws.com |  The Amazon ECS endpoint to use. | https://ecs.us-east-1.amazonaws.com |