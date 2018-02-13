<?php
/**
 * Created by A.
 * User: ahmetgunes
 * Date: 12.02.2018
 * Time: 23:42
 */

namespace BeanStalkEvent;

use Beanstalk\Client;
use ScheduledEvent\Model\Exception\ScheduledEventException;
use ScheduledEvent\Model\Manager\QueueManagerInterface;
use ScheduledEvent\Model\Message\MessageInterface;
use ScheduledEvent\Model\Router\RouterInterface;

class BeanStalkManager implements QueueManagerInterface
{
    /**
     * @var Client
     */
    protected $mq;

    /**
     * @var RouterInterface
     */
    protected $router;

    /**
     * BeanStalkManager constructor.
     * @param RouterInterface $router
     * @param Client $mq
     */
    public function __construct(RouterInterface $router, Client $mq)
    {
        $this->router = $router;
        $this->mq = $mq;
    }

    public function publish(MessageInterface $message): bool
    {
        if (!$message instanceof BeanStalkMessage) {
            throw new ScheduledEventException('Wrong formatted message is passed to the manager.');
        }

        list($payload, $priority) = BeanStalkMessageSerializer::convert($message);

        return $this->mq->put($priority, 0, 60, $payload);
    }

    public function consume()
    {
        while (true) {
            $job = $this->mq->reserve();
            $message = BeanStalkMessageSerializer::deConvert($job);
            //If designated date didn't arrive yet delete the job and republish
            if (!is_null($message->getDesignatedDate()) && $message->getDesignatedDate() > time()) {
                $this->mq->delete($job['id']);
                $this->publish($message);
            } else {
                $this->router->route($message);
                $this->mq->bury($job['id'], 1);
            }
        }
    }
}