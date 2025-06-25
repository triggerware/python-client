from abc import ABC, abstractmethod

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import triggerware as tw
    import triggerware.jrpc as jrpc

from triggerware.queries import AbstractQuery
from triggerware.interfaces import TriggerwareObject

class Subscription[T](ABC, AbstractQuery[T]):
    """
    A Subscription represents some future change to the data managed by the TW server about which
    a client would like to be notified. Once created, this subscription will accept notifications from
    the server when a change occurs, then call the overridden method handleNotification.

    By default, subscriptions are **active** when they are created—they are immediately registered
    with the server and start receiving notifications. This behavior may be changed upon construction,
    or by calling either the activate or deactivate methods.

    Subscriptions may either be created by passing in a TriggerwareClient, in which case they
    are immediately registered with the server, OR by passing in a BatchSubscription. See
    BatchSubscription documentation for more information.
    """
    _batch: "BatchSubscription | None"
    label: str
    _active: bool
    _handler: "jrpc.JsonRpcMessageHandler" 

    @property
    def active(self) -> bool:
        return self._active

    @property
    def part_of_batch(self) -> bool:
        return self._batch is not None

    def __init__(
        self,
        client_or_batch: "tw.TriggerwareClient | tw.BatchSubscription",
        query: "tw.Query",
        active: bool = True
    ):
        """
        Initialize a Subscription instance.

        Args:
            client_or_batch (tw.TriggerwareClient | tw.BatchSubscription): The client or batch to register with.
            query (tw.Query): The query to subscribe to.
            active (bool, optional): Whether the subscription should be active upon creation. Defaults to True.
        """
        from triggerware.triggerware_client import TriggerwareClient
        from triggerware.jrpc import JsonRpcMessageHandler
        client = client_or_batch if isinstance(client_or_batch, TriggerwareClient) else client_or_batch.client
        super().__init__(client, query)
        self._active = False
        self._batch = None
        self.label = "sub" + str(client._sub_counter) # type: ignore
        self._handler = JsonRpcMessageHandler(
            lambda _: {},
            lambda x: self.handle_notification(x['tuple']) # type: ignore
        )
        client._sub_counter += 1 # type: ignore
        self.base_parameters["label"] = self.label


        if isinstance(client_or_batch, TriggerwareClient):
            if active:
                self.activate()
        else:
            self.add_to_batch(client_or_batch)

    def activate(self):
        """
        Activates the subscription, enabling notifications to be sent from the server.

        Raises:
            SubscriptionException: If the subscription is already active or is part of a batch.
        """
        from triggerware.types import SubscriptionException
        if self._batch:
            raise SubscriptionException("Cannot activate subscription that is part of a batch.")

        if self._active:
            raise SubscriptionException("Subscription is already active.")

        params = {**self.base_parameters, "method": self.label, "combine": False}
        self.client.json_rpc.call("subscribe", params)
        self.client.json_rpc.add_method(self.label, self._handler)
        self._active = True

    def deactivate(self):
        """
        Deactivates the subscription, disabling notifications from the server.

        Raises:
            SubscriptionException: If the subscription is already inactive or is part of a batch.
        """
        from triggerware.types import SubscriptionException
        if self._batch:
            raise SubscriptionException("Cannot deactivate a subscription that is part of a batch.")

        if not self._active:
            raise SubscriptionException("Subscription is already inactive.")

        params = {**self.base_parameters, "method": self.label, "combine": False}
        self.client.json_rpc.call("unsubscribe", params)
        self.client.json_rpc.remove_method(self.label)
        self._active = False

    def add_to_batch(self, batch: "BatchSubscription"):
        """
        Adds the subscription to the provided batch. Alternatively, you may call a batch's 
        add_subscription method.

        Args:
            batch (BatchSubscription): The batch to add this subscription to.

        Raises:
            SubscriptionException: If the subscription is already active, already part of a batch, or registered with a different client.
        """
        from triggerware.types import SubscriptionException
        if self._active:
            raise SubscriptionException("Cannot add active subscription to a batch.")

        if self._batch:
            raise SubscriptionException("Subscription is already part of another batch.")

        if not self.client is batch.client:
            raise SubscriptionException("Subscription and batch registered with different clients.")

        params = {**self.base_parameters, "method": batch.method_name, "combine": True}
        self.client.json_rpc.call("subscribe", params)
        batch._subscriptions[self.label] = self # type: ignore
        self._batch = batch

    def remove_from_batch(self):
        """
        Removes the subscription from its current batch. Alternatively, you may call a batch's
        remove_subscription method.

        Raises:
            SubscriptionException: If the subscription is not part of a batch.
        """
        from triggerware.types import SubscriptionException
        if not self._batch:
            raise SubscriptionException("Subscription is not part of a batch.")

        params = {**self.base_parameters, "method": self._batch.method_name, "combine": True}
        self.client.json_rpc.call("unsubscribe", params)
        if self.label in self._batch._subscriptions: # type: ignore
            del self._batch._subscriptions[self.label] # type: ignore
        self._batch = None

    def _handle_notification_from_batch(self, data: list[T]):
        """
        Handles notifications from a batch subscription and dispatches them to the handler.

        Args:
            data (list[T]): The data received in the notification.
        """
        for d in data:
            self.handle_notification(d)
    
    @abstractmethod
    def handle_notification(self, data: T):
        """
        Overload this function in an inheriting class to handle notifications that triggering the
        query will cause this function to activate.

        Args:
            data (T): The data received in the notification.
        """
        pass


class BatchSubscription(TriggerwareObject):
    """
    A BatchSubscription groups one or more Subscription instances. Over time, new instances
    may be added to the BatchSubscription, and/or existing members may be removed. This is useful
    because a single transaction of a change in data on the triggerware server may be associated with
    multiple subscriptions.

    By grouping these subscriptions, notifications may be properly handled by as many Subscription
    instances as necessary.
    """
    _subscriptions: dict[str, Subscription]
    method_name: str

    def __init__(self, client: "tw.TriggerwareClient"):
        """
        Initialize a BatchSubscription instance.

        Args:
            client (tw.TriggerwareClient): The Triggerware client.
        """
        from triggerware.jrpc import JsonRpcMessageHandler
        self.client = client
        self.method_name = "batch" + str(client._batch_sub_counter) # type: ignore
        client._batch_sub_counter += 1 # type: ignore
        self._subscriptions = {}

        def notify_handler(message):
            for match in message['matches']:
                subscription = self._subscriptions.get(match['label'])
                if subscription:
                    subscription._handle_notification_from_batch(match['tuples']) # type: ignore

        handler = JsonRpcMessageHandler(lambda _: {}, notify_handler) 
        self.client.json_rpc.add_method(self.method_name, handler)


    def add_subscription(self, subscription: Subscription):
        """
        Adds a subscription to the batch. Alternatively, you may call a subscription's
        add_to_batch method.

        Args:
            subscription (Subscription): The subscription to add.
        """
        subscription.add_to_batch(self)

    def remove_subscription(self, subscription: Subscription):
        """
        Removes a subscription from the batch. Alternatively, you may call a subscription's
        remove_from_batch method.

        Args:
            subscription (Subscription): The subscription to remove.

        Raises:
            SubscriptionException: If the subscription is not part of this batch.
        """
        from triggerware.types import SubscriptionException
        if subscription.label not in self._subscriptions:
            raise SubscriptionException("Subscription is not part of this batch.")

        subscription.remove_from_batch()







