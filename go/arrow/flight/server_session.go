// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flight

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	SessionCookieName          string = "arrow_flight_session_id"
	StatelessSessionCookieName string = "arrow_flight_session"
)

var (
	ErrNoSession error = errors.New("flight: server session not present")
)

type sessionMiddlewareKey struct{}

// Return a copy of the provided context containing the provided ServerSession
func NewSessionContext(ctx context.Context, session ServerSession) context.Context {
	return context.WithValue(ctx, sessionMiddlewareKey{}, session)
}

// Retrieve the ServerSession from the provided context if it exists.
// An error indicates that the session was not found in the context.
func GetSessionFromContext(ctx context.Context) (ServerSession, error) {
	session, ok := ctx.Value(sessionMiddlewareKey{}).(ServerSession)
	if !ok {
		return nil, ErrNoSession
	}
	return session, nil
}

// Check the provided context for cookies in the incoming gRPC metadata.
func GetSessionIDFromIncomingCookie(ctx context.Context) (string, error) {
	cookie, err := GetIncomingCookieByName(ctx, SessionCookieName)
	if err != nil {
		return "", err
	}

	return cookie.Value, nil
}

// Check the provided context for cookies in the incoming gRPC metadata.
func GetSessionFromIncomingCookie(ctx context.Context) (*statelessServerSession, error) {
	cookie, err := GetIncomingCookieByName(ctx, StatelessSessionCookieName)
	if err != nil {
		return nil, err
	}

	return DecodeStatelessToken(cookie.Value)
}

func GetIncomingCookieByName(ctx context.Context, name string) (http.Cookie, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return http.Cookie{}, fmt.Errorf("no metadata found for incoming context")
	}

	header := make(http.Header, md.Len())
	for k, v := range md {
		for _, val := range v {
			header.Add(k, val)
		}
	}

	cookie, err := (&http.Request{Header: header}).Cookie(name)
	if err != nil {
		return http.Cookie{}, err
	}

	if cookie == nil {
		return http.Cookie{}, fmt.Errorf("failed to get cookie with name: %s", name)
	}

	return *cookie, nil
}

func CreateCookieForSession(session ServerSession) (http.Cookie, error) {
	var key string

	if session == nil {
		return http.Cookie{}, ErrNoSession
	}

	switch s := session.(type) {
	case *statefulServerSession:
		key = SessionCookieName
	case *statelessServerSession:
		key = StatelessSessionCookieName
	default:
		return http.Cookie{}, fmt.Errorf("cannot serialize session of type %T as cookie", s)
	}

	req := http.Request{Header: http.Header{"Cookie": []string{fmt.Sprintf("%s=%s", key, session.Token())}}}
	cookie, err := req.Cookie(key) // TODO: one line
	if err != nil {
		return http.Cookie{}, err
	}
	if cookie == nil {
		return http.Cookie{}, fmt.Errorf("failed to construct cookie for session: %s", session.Token())
	}

	return *cookie, nil
}

// Persistence of ServerSession instances for stateful session implementations
type SessionStore interface {
	// Get the session with the provided ID
	Get(id string) (ServerSession, error)
	// Persist the provided session
	Put(session ServerSession) error
	// Remove the session with the provided ID
	Remove(id string) error
}

// Creation of ServerSession instances
type SessionFactory interface {
	// Create a new, empty ServerSession
	CreateSession() (ServerSession, error)
}

// Container for named SessionOptionValues
type ServerSession interface {
	// An identifier for the session that the server can use to reconstruct
	// the session state on future requests. It is the responsibility of
	// each implementation to define the token's semantics.
	Token() string
	// Get session option value by name, or nil if it does not exist
	GetSessionOption(name string) *SessionOptionValue
	// Get a copy of the session options
	GetSessionOptions() map[string]*SessionOptionValue
	// Set session option by name to given value
	SetSessionOption(name string, value *SessionOptionValue)
	// Idempotently remove name from this session
	EraseSessionOption(name string)
	// Close the session
	Close() error
	// Report whether the session has been closed
	Closed() bool
}

// Handles session lifecycle management
type ServerSessionManager interface {
	// Create a new, empty ServerSession
	CreateSession(ctx context.Context) (ServerSession, error)
	// Get the current ServerSession, if one exists
	GetSession(ctx context.Context) (ServerSession, error)
	// Cleanup any resources associated with the current ServerSession
	CloseSession(session ServerSession) error
}

// Creates a simple in-memory, goroutine-safe SessionStore
func NewSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]ServerSession)}
}

type sessionStore struct {
	sessions map[string]ServerSession
	mu       sync.RWMutex
}

func (store *sessionStore) Get(id string) (ServerSession, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	session, found := store.sessions[id]
	if !found {
		return nil, fmt.Errorf("session with ID %s not found", id)
	}
	return session, nil
}

func (store *sessionStore) Put(session ServerSession) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sessions[session.Token()] = session
	return nil
}

func (store *sessionStore) Remove(id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.sessions, id)

	return nil
}

// Create a new SessionFactory, producing in-memory, goroutine-safe ServerSessions.
// The provided function MUST produce collision-free identifiers.
func NewSessionFactory(generateID func() string) *sessionFactory {
	return &sessionFactory{generateID: generateID}
}

type sessionFactory struct {
	generateID func() string
}

func (factory *sessionFactory) CreateSession() (ServerSession, error) {
	return &statefulServerSession{
		id:            factory.generateID(),
		serverSession: serverSession{options: make(map[string]*SessionOptionValue)},
	}, nil
}

type statefulServerSession struct {
	serverSession
	id string
}

func (session *statefulServerSession) Token() string {
	return session.id
}

func NewStatelessServerSession() *statelessServerSession {
	return &statelessServerSession{
		serverSession: serverSession{options: make(map[string]*SessionOptionValue)},
	}
}

type statelessServerSession struct {
	serverSession
}

func (session *statelessServerSession) Token() string {
	payload := GetSessionOptionsResult{SessionOptions: session.options}
	b, err := proto.Marshal(&payload)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal stateless token: %s", err))
	}

	return base64.StdEncoding.EncodeToString(b)
}

func DecodeStatelessToken(token string) (*statelessServerSession, error) {
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	var parsed GetSessionOptionsResult
	if err := proto.Unmarshal(decoded, &parsed); err != nil {
		return nil, err
	}

	options := parsed.SessionOptions
	if options == nil {
		options = make(map[string]*SessionOptionValue)
	}

	return &statelessServerSession{
		serverSession: serverSession{options: options},
	}, nil
}

type serverSession struct {
	closed bool

	options map[string]*SessionOptionValue
	mu      sync.RWMutex
}

func (session *serverSession) GetSessionOption(name string) *SessionOptionValue {
	session.mu.RLock()
	defer session.mu.RUnlock()
	value, found := session.options[name]
	if !found {
		return nil
	}

	return value
}

func (session *serverSession) GetSessionOptions() map[string]*SessionOptionValue {
	options := make(map[string]*SessionOptionValue, len(session.options))

	session.mu.RLock()
	defer session.mu.RUnlock()
	for k, v := range session.options {
		options[k] = v
	}

	return options
}

func (session *serverSession) SetSessionOption(name string, value *SessionOptionValue) {
	if value.GetOptionValue() == nil {
		session.EraseSessionOption(name)
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	session.options[name] = value
}

func (session *serverSession) EraseSessionOption(name string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	delete(session.options, name)
}

func (session *serverSession) Close() error {
	session.options = nil
	session.closed = true
	return nil
}

func (session *serverSession) Closed() bool {
	return session.closed
}

type SessionManagerOption func(*serverSessionManager)

// WithFactory specifies the SessionFactory to use for session creation
func WithFactory(factory SessionFactory) SessionManagerOption {
	return func(manager *serverSessionManager) {
		manager.factory = factory
	}
}

// WithStore specifies the SessionStore to use for session persistence
func WithStore(store SessionStore) SessionManagerOption {
	return func(manager *serverSessionManager) {
		manager.store = store
	}
}

// Create a new ServerSessionManager
// If unset via options, the default factory produces sessions with UUIDs.
// If unset via options, sessions are stored in-memory.
func NewServerSessionManager(opts ...SessionManagerOption) *serverSessionManager {
	manager := &serverSessionManager{}
	for _, opt := range opts {
		opt(manager)
	}

	// Set defaults if not specified above
	if manager.factory == nil {
		manager.factory = NewSessionFactory(uuid.NewString)
	}

	if manager.store == nil {
		manager.store = NewSessionStore()
	}

	return manager
}

type serverSessionManager struct {
	factory SessionFactory
	store   SessionStore
}

func (manager *serverSessionManager) CreateSession(ctx context.Context) (ServerSession, error) {
	session, err := manager.factory.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create new session: %w", err)
	}

	if err = manager.store.Put(session); err != nil {
		return nil, fmt.Errorf("failed to persist new session: %w", err)
	}

	return session, nil
}

func (manager *serverSessionManager) GetSession(ctx context.Context) (ServerSession, error) {
	session, err := GetSessionFromContext(ctx)
	if err == nil {
		return session, nil
	}

	sessionID, err := GetSessionIDFromIncomingCookie(ctx)
	if err == nil {
		return manager.store.Get(sessionID)
	}
	if err == http.ErrNoCookie {
		return nil, ErrNoSession
	}

	return nil, fmt.Errorf("failed to get current session from cookie: %w", err)
}

func (manager *serverSessionManager) CloseSession(session ServerSession) error {
	if err := manager.store.Remove(session.Token()); err != nil {
		return fmt.Errorf("failed to remove server session from store: %w", err)
	}
	return nil
}

// Create a new ServerSessionManager
// If unset via options, the default factory produces sessions with UUIDs.
// If unset via options, sessions are stored in-memory.
func NewStatelessServerSessionManager() *statelessServerSessionManager {
	return &statelessServerSessionManager{}
}

type statelessServerSessionManager struct{}

func (manager *statelessServerSessionManager) CreateSession(ctx context.Context) (ServerSession, error) {
	return NewStatelessServerSession(), nil
}

func (manager *statelessServerSessionManager) GetSession(ctx context.Context) (ServerSession, error) {
	session, err := GetSessionFromContext(ctx)
	if err == nil {
		return session, nil
	}

	session, err = GetSessionFromIncomingCookie(ctx)
	if err == nil {
		return session, err
	}
	if err == http.ErrNoCookie {
		return nil, ErrNoSession
	}

	return nil, fmt.Errorf("failed to get current session from cookie: %w", err)
}

func (manager *statelessServerSessionManager) CloseSession(session ServerSession) error {
	return nil
}

// Create new instance of CustomServerMiddleware implementing server session persistence.
//
// The provided manager can be used to customize session implementation/behavior.
// If no manager is provided, default in-memory, goroutine-safe implementation is used.
func NewServerSessionMiddleware(manager ServerSessionManager) *serverSessionMiddleware {
	// Default manager
	if manager == nil {
		manager = NewServerSessionManager()
	}
	return &serverSessionMiddleware{manager: manager}
}

type serverSessionMiddleware struct {
	manager ServerSessionManager
}

func (middleware *serverSessionMiddleware) StartCall(ctx context.Context) context.Context {
	session, err := middleware.manager.GetSession(ctx)
	if err == nil {
		return NewSessionContext(ctx, session)
	}

	if err != ErrNoSession {
		panic(err)
	}

	session, err = middleware.manager.CreateSession(ctx)
	if err != nil {
		panic(err)
	}

	return NewSessionContext(ctx, session)
}

// Determine if the session state has changed. If it has then we need to inform the client
// with a new cookie. The cookie is sent in the gRPC trailer because we would like to
// determine its contents based on the final state the session at the end of the RPC call.
func (middleware *serverSessionMiddleware) CallCompleted(ctx context.Context, _ error) {
	session, err := middleware.manager.GetSession(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get server session: %s", err))
	}

	sessionCookie, err := CreateCookieForSession(session)
	if err != nil {
		panic(err)
	}

	clientCookie, err := GetIncomingCookieByName(ctx, sessionCookie.Name)
	if err == http.ErrNoCookie {
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", sessionCookie.String()))
		return
	}

	if err != nil {
		panic(err)
	}

	if session.Closed() {
		clientCookie.MaxAge = -1
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", clientCookie.String()))

		if err = middleware.manager.CloseSession(session); err != nil {
			panic(fmt.Sprintf("failed to close server session: %s", err))
		}
		return
	}

	if sessionCookie.String() != clientCookie.String() {
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", sessionCookie.String()))
	}
}
